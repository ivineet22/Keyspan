package router

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"store/internal/hash"
	"store/internal/store"
	"store/internal/wire"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

var (
	ErrNotFound    = wire.ErrNotFound
	ErrWrongShard  = wire.ErrWrongShard
	ErrStaleEpoch  = wire.ErrStaleEpoch
	ErrUnavailable = wire.ErrUnavailable
	ErrRetry       = wire.ErrRetry
)

// Shard is one C++ process. Name is what Postgres stores as the owner.
// Addr is host:port where that process listens.
type Shard struct {
	Name string
	Addr string
}

type Router struct {
	db       *store.Store
	names    []string
	addrs    map[string]string
	cache    map[string][]store.Range
	mu       sync.Mutex
	rdb      *redis.Client
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	reg      *prometheus.Registry
}

func ParseShards(spec string) ([]Shard, error) {
	if spec == "" {
		spec = "shard-1=127.0.0.1:50051,shard-2=127.0.0.1:50052,shard-3=127.0.0.1:50053"
	}
	var out []Shard
	for _, part := range strings.Split(spec, ",") {
		name, addr, ok := strings.Cut(part, "=")
		if !ok || name == "" || addr == "" {
			return nil, fmt.Errorf("bad shard entry %q", part)
		}
		out = append(out, Shard{Name: name, Addr: addr})
	}
	return out, nil
}

func New(db *store.Store, shards []Shard, rdb *redis.Client) *Router {
	reg := prometheus.NewRegistry()
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "store_requests_total",
		Help: "Requests the router forwarded, labeled by shard and result.",
	}, []string{"shard", "result"})
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "store_request_duration_seconds",
		Help: "Time the router spent forwarding one request.",
	}, []string{"shard"})
	reg.MustRegister(requests, latency)
	r := &Router{
		db:       db,
		addrs:    make(map[string]string, len(shards)),
		cache:    make(map[string][]store.Range),
		rdb:      rdb,
		requests: requests,
		latency:  latency,
		reg:      reg,
	}
	for _, s := range shards {
		r.names = append(r.names, s.Name)
		r.addrs[s.Name] = s.Addr
	}
	return r
}

// DialRedis connects to Redis. A failure here is reported to the caller.
// Serving still works when the caller passes a nil client to New.
func DialRedis(addr string) (*redis.Client, error) {
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

// Metrics serves the Prometheus text that store_requests_total lives in.
func (r *Router) Metrics() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}

func (r *Router) Close() {}

func (r *Router) Put(ctx context.Context, tenant, key string, value []byte) error {
	return r.call(ctx, tenant, key, func(addr string, epoch int64) error {
		return wire.Put(ctx, addr, tenant, key, value, epoch)
	})
}

func (r *Router) Get(ctx context.Context, tenant, key string) ([]byte, error) {
	var value []byte
	err := r.call(ctx, tenant, key, func(addr string, epoch int64) error {
		got, err := wire.Get(ctx, addr, tenant, key, epoch)
		if err != nil {
			return err
		}
		value = got
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// call looks up the owner and tries once. A stale epoch, a wrong shard, or a
// dead process means the remembered owner may be old, so it reads Postgres
// again. If Postgres still names that same dead shard, it does not try another.
func (r *Router) call(ctx context.Context, tenant, key string, fn func(string, int64) error) error {
	start := time.Now()
	rg, err := r.locate(ctx, tenant, key, false)
	if err != nil {
		r.observe("none", start, err)
		return err
	}
	err = r.forward(rg, fn)
	dead := errors.Is(err, ErrUnavailable)
	if err == nil || (!errors.Is(err, ErrStaleEpoch) && !errors.Is(err, ErrWrongShard) && !dead) {
		r.observe(rg.Owner, start, err)
		r.bump(ctx, rg)
		return err
	}
	prev := rg.Owner
	rg, err = r.locate(ctx, tenant, key, true)
	if err != nil {
		r.observe("none", start, err)
		return err
	}
	if dead && rg.Owner == prev {
		r.observe(prev, start, ErrUnavailable)
		r.bump(ctx, rg)
		return ErrUnavailable
	}
	err = r.forward(rg, fn)
	r.observe(rg.Owner, start, err)
	r.bump(ctx, rg)
	return err
}

func (r *Router) forward(rg store.Range, fn func(string, int64) error) error {
	addr, err := r.addr(rg.Owner)
	if err != nil {
		return err
	}
	return fn(addr, rg.Epoch)
}

func (r *Router) observe(shard string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	r.requests.WithLabelValues(shard, result).Inc()
	r.latency.WithLabelValues(shard).Observe(time.Since(start).Seconds())
}

// bump counts one request against the range that owned it.
// The key expires quickly so a quiet range disappears.
// A Redis failure does not fail the request.
func (r *Router) bump(ctx context.Context, rg store.Range) {
	if r.rdb == nil {
		return
	}
	key := fmt.Sprintf("store:range:%s:%d", rg.Tenant, rg.Start)
	pipe := r.rdb.Pipeline()
	pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 60*time.Second)
	_, _ = pipe.Exec(ctx)
}

func (r *Router) addr(name string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	addr, ok := r.addrs[name]
	if !ok || addr == "" {
		return "", ErrUnavailable
	}
	return addr, nil
}

func (r *Router) locate(ctx context.Context, tenant, key string, refresh bool) (store.Range, error) {
	place := int64(hash.Key(tenant, key))
	if !refresh {
		r.mu.Lock()
		cached := r.cache[tenant]
		r.mu.Unlock()
		if rg, ok := cover(cached, place); ok {
			return rg, nil
		}
	}
	ranges, err := r.db.EnsureTenant(ctx, tenant, r.names)
	if err != nil {
		return store.Range{}, err
	}
	r.mu.Lock()
	r.cache[tenant] = ranges
	r.mu.Unlock()
	rg, ok := cover(ranges, place)
	if !ok {
		return store.Range{}, fmt.Errorf("no range covers hash %d", place)
	}
	return rg, nil
}

func cover(ranges []store.Range, place int64) (store.Range, bool) {
	for _, rg := range ranges {
		if place >= rg.Start && place < rg.End {
			return rg, true
		}
	}
	return store.Range{}, false
}
