package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"store/internal/api"
	"store/internal/hash"
	"store/internal/load"
	"store/internal/router"
	"store/internal/store"
	"store/internal/wire"
)

func TestPutSurvivesShardRestart(t *testing.T) {
	db, shards, srv := start(t)
	url := srv.URL + "/v1/tenants/acme/keys/user-1"
	put(t, url, "hello")

	owner := ownerOf(t, db, "acme", "user-1")
	var owned *proc
	for i := range shards {
		if shards[i].name == owner {
			owned = &shards[i]
		}
	}
	if owned == nil {
		t.Fatalf("owner %s is not running", owner)
	}
	owned.stop()
	owned.start(t)

	if got := get(t, srv.URL+"/v1/tenants/acme/keys/user-1"); got != "hello" {
		t.Fatalf("after restart got %q", got)
	}
}

func TestRequestToOtherShardFails(t *testing.T) {
	db, shards, srv := start(t)
	put(t, srv.URL+"/v1/tenants/acme/keys/user-1", "hello")
	owner := ownerOf(t, db, "acme", "user-1")

	var other *proc
	for i := range shards {
		if shards[i].name != owner {
			other = &shards[i]
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := wire.Get(ctx, other.addr, "acme", "user-1", 1)
	if !errors.Is(err, wire.ErrWrongShard) {
		t.Fatalf("got %v", err)
	}
}

func TestStaleEpochIsRejected(t *testing.T) {
	db, shards, srv := start(t)
	put(t, srv.URL+"/v1/tenants/acme/keys/user-1", "hello")
	owner := ownerOf(t, db, "acme", "user-1")
	var owned *proc
	for i := range shards {
		if shards[i].name == owner {
			owned = &shards[i]
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := wire.Put(ctx, owned.addr, "acme", "user-1", []byte("nope"), 99)
	if !errors.Is(err, wire.ErrStaleEpoch) {
		t.Fatalf("got %v", err)
	}
	if got := get(t, srv.URL+"/v1/tenants/acme/keys/user-1"); got != "hello" {
		t.Fatalf("value changed to %q", got)
	}
}

func TestGetMissingKey(t *testing.T) {
	_, _, srv := start(t)
	res, err := http.Get(srv.URL + "/v1/tenants/acme/keys/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestPutReplacesValue(t *testing.T) {
	_, _, srv := start(t)
	url := srv.URL + "/v1/tenants/acme/keys/replace-me"
	put(t, url, "one")
	put(t, url, "two")
	if got := get(t, url); got != "two" {
		t.Fatalf("got %q", got)
	}
}

func TestScanReturnsTheLoggedPut(t *testing.T) {
	db, shards, srv := start(t)
	put(t, srv.URL+"/v1/tenants/acme/keys/user-1", "hello")
	ranges, err := db.EnsureTenant(context.Background(), "acme", names(shards))
	if err != nil {
		t.Fatal(err)
	}
	place := int64(hash.Key("acme", "user-1"))
	var rg store.Range
	var owned *proc
	for _, candidate := range ranges {
		if place >= candidate.Start && place < candidate.End {
			rg = candidate
		}
	}
	for i := range shards {
		if shards[i].name == rg.Owner {
			owned = &shards[i]
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pos, err := wire.Sync(ctx, owned.addr)
	if err != nil {
		t.Fatal(err)
	}
	if pos == 0 {
		t.Fatal("sync returned position 0")
	}
	records, err := wire.Scan(ctx, owned.addr, "acme", rg.Start, rg.End, 0, 0, rg.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	var saw bool
	for _, rec := range records {
		if rec.Key == "user-1" && string(rec.Value) == "hello" && !rec.Deleted {
			saw = true
		}
	}
	if !saw {
		t.Fatal("scan did not return the put")
	}
}

func TestSkewLandsOnOneShard(t *testing.T) {
	rdb, err := router.DialRedis("")
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })
	ctx := context.Background()
	iter := rdb.Scan(ctx, 0, "store:range:*", 100).Iterator()
	var stale []string
	for iter.Next(ctx) {
		stale = append(stale, iter.Val())
	}
	if len(stale) > 0 {
		rdb.Del(ctx, stale...)
	}

	db, shards, _ := start(t)
	rt := router.New(db, []router.Shard{
		{Name: shards[0].name, Addr: shards[0].addr},
		{Name: shards[1].name, Addr: shards[1].addr},
		{Name: shards[2].name, Addr: shards[2].addr},
	}, rdb)
	t.Cleanup(rt.Close)
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", rt.Metrics())
	mux.Handle("/", api.New(rt))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	report, err := load.Run(ctx, srv.URL, 400)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", "results", "baseline.txt")
	if err := load.WriteBaseline(path, report); err != nil {
		t.Fatal(err)
	}
	if report.Errors != 0 {
		t.Fatalf("errors %d", report.Errors)
	}
	if report.Share < 0.6 {
		t.Fatalf("hottest share %.2f, counts %v", report.Share, report.ByShard)
	}
	for shard, n := range report.ByShard {
		if shard != report.Hottest && n >= report.Max {
			t.Fatalf("shard %s tied the hottest: %v", shard, report.ByShard)
		}
	}
	hotRange, err := rdb.Get(ctx, "store:range:hot:0").Int()
	if err != nil {
		t.Fatal(err)
	}
	if hotRange < 240 {
		t.Fatalf("redis count for the hot range = %d", hotRange)
	}
}

func TestValuesAreNotInPostgres(t *testing.T) {
	db, _, srv := start(t)
	put(t, srv.URL+"/v1/tenants/acme/keys/user-1", "hello")
	gone, err := db.RecordsGone(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !gone {
		t.Fatal("records table still exists")
	}
}

type proc struct {
	name string
	addr string
	dir  string
	bin  string
	dsn  string
	cmd  *exec.Cmd
	log  *os.File
}

func (p *proc) start(t *testing.T) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(p.dir, "process.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	p.cmd = exec.Command(p.bin,
		"--name", p.name,
		"--listen", p.addr,
		"--data", p.dir,
		"--database-url", p.dsn,
	)
	p.cmd.Stdout = f
	p.cmd.Stderr = f
	env := os.Environ()
	pg := `C:\Program Files\PostgreSQL\17\bin`
	p.cmd.Env = append(env, "POSTGRES_BIN="+pg)
	if err := p.cmd.Start(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	p.log = f
	if err := waitListen(p.addr); err != nil {
		p.stop()
		body, _ := os.ReadFile(filepath.Join(p.dir, "process.log"))
		t.Fatalf("start %s: %v\n%s", p.name, err, body)
	}
}

func (p *proc) stop() {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
	}
	if p.log != nil {
		_ = p.log.Close()
		p.log = nil
	}
}

func start(t *testing.T) (*store.Store, []proc, *httptest.Server) {
	t.Helper()
	bin := filepath.Join("..", "..", "bin", "shard.exe")
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("missing %s (%v). Build the shard before this test.", bin, err)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:devpass@127.0.0.1:5432/store_test?sslmode=disable"
	}
	db, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.RemoveAllRanges(context.Background()); err != nil {
		t.Fatal(err)
	}

	shards := make([]proc, 3)
	for i := 0; i < 3; i++ {
		shards[i] = proc{
			name: fmt.Sprintf("shard-%d", i+1),
			addr: freeAddr(t),
			dir:  t.TempDir(),
			bin:  bin,
			dsn:  dsn,
		}
		shards[i].start(t)
	}
	t.Cleanup(func() {
		for i := range shards {
			shards[i].stop()
		}
	})

	rt := router.New(db, []router.Shard{
		{Name: shards[0].name, Addr: shards[0].addr},
		{Name: shards[1].name, Addr: shards[1].addr},
		{Name: shards[2].name, Addr: shards[2].addr},
	}, nil)
	t.Cleanup(rt.Close)
	srv := httptest.NewServer(api.New(rt))
	t.Cleanup(srv.Close)
	return db, shards, srv
}

func names(shards []proc) []string {
	out := make([]string, len(shards))
	for i := range shards {
		out[i] = shards[i].name
	}
	return out
}

func ownerOf(t *testing.T, db *store.Store, tenant, key string) string {
	t.Helper()
	ranges, err := db.EnsureTenant(context.Background(), tenant, []string{"shard-1", "shard-2", "shard-3"})
	if err != nil {
		t.Fatal(err)
	}
	place := int64(hash.Key(tenant, key))
	for _, rg := range ranges {
		if place >= rg.Start && place < rg.End {
			return rg.Owner
		}
	}
	t.Fatalf("no owner for %s/%s", tenant, key)
	return ""
}

func put(t *testing.T, url, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("put status %d", res.StatusCode)
	}
}

func get(t *testing.T, url string) string {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("get status %d body %s", res.StatusCode, body)
	}
	return string(body)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func waitListen(addr string) error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("%s did not listen", addr)
}
