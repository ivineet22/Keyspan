package api_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"store/internal/api"
	"store/internal/balance"
	"store/internal/load"
	"store/internal/router"
	"store/internal/store"
	"store/internal/worker"

	"github.com/redis/go-redis/v9"
)

func TestRebalanceCalmsTheHotShard(t *testing.T) {
	rdb, err := router.DialRedis("")
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })
	ctx := context.Background()
	flushRanges(t, rdb)

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

	b := &balance.Balancer{DB: db, Redis: rdb, Owners: names(shards), CopyCap: 100000}
	var before load.Report
	var last load.Report
	resumed := 0
	restarted := false
	for pass := 0; pass < 24; pass++ {
		snap := metricSnap(t, srv.URL+"/metrics")
		report, err := load.Run(ctx, srv.URL, 400)
		if err != nil {
			t.Fatal(err)
		}
		report = load.Isolate(report, snap)
		if report.Errors != 0 {
			t.Fatalf("pass %d errors %d", pass, report.Errors)
		}
		if pass == 0 {
			before = report
		}
		last = report
		if err := b.Observe(ctx); err != nil {
			t.Fatal(err)
		}
		act, err := b.Plan(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("pass %d share %.4f max/mean %.2f action %s %s [%d,%d) -> %s",
			pass, report.Share, ratio(report), act.Op, act.Tenant, act.Start, act.End, act.Destination)
		if act.Op == "none" {
			break
		}
		if !restarted {
			stop := &worker.Worker{
				DB: db, Addrs: addrs(shards), ID: fmt.Sprintf("stop-%d", pass),
				Lease: 300 * time.Millisecond, StopAfter: store.StepSnapshotting, CopyCap: b.CopyCap,
			}
			if err := stop.Run(ctx); !errors.Is(err, worker.ErrStopped) {
				t.Fatalf("stop worker: %v", err)
			}
			time.Sleep(500 * time.Millisecond)
			resumed++
			restarted = true
		}
		run := &worker.Worker{
			DB: db, Addrs: addrs(shards), ID: fmt.Sprintf("run-%d", pass),
			Lease: 15 * time.Second, CopyCap: b.CopyCap,
		}
		if err := run.Run(ctx); err != nil {
			t.Fatal(err)
		}
		flushRanges(t, rdb)
	}
	copied, err := db.RecordsCopied(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out := load.Outcome{
		Report:        last,
		RecordsCopied: copied,
		Splits:        b.Splits,
		MovesResumed:  resumed,
	}
	path := filepath.Join("..", "..", "results", "rebalanced.txt")
	if err := load.WriteRebalanced(path, out); err != nil {
		t.Fatal(err)
	}
	t.Logf("before share %.4f max/mean %.2f", before.Share, ratio(before))
	t.Logf("after share %.4f max/mean %.2f records_copied %d splits %d moves_resumed %d",
		last.Share, ratio(last), copied, b.Splits, resumed)
	if resumed < 1 {
		t.Fatal("worker was not restarted")
	}
	if b.Splits < 1 {
		t.Fatal("balancer never split the hot range")
	}
	if copied < 1 {
		t.Fatal("no records were copied")
	}
	if last.Share >= before.Share {
		t.Fatalf("hottest share %.4f did not fall below %.4f", last.Share, before.Share)
	}
	if ratio(last) >= ratio(before) {
		t.Fatalf("max/mean %.2f did not fall below %.2f", ratio(last), ratio(before))
	}
}

func ratio(r load.Report) float64 {
	if r.Mean == 0 {
		return 0
	}
	return r.Max / r.Mean
}

func metricSnap(t *testing.T, url string) map[string]float64 {
	t.Helper()
	counts, err := load.ShardCounts(context.Background(), url)
	if err != nil {
		return map[string]float64{}
	}
	return counts
}

func flushRanges(t *testing.T, rdb *redis.Client) {
	t.Helper()
	ctx := context.Background()
	iter := rdb.Scan(ctx, 0, "store:range:*", 100).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if len(keys) > 0 {
		if err := rdb.Del(ctx, keys...).Err(); err != nil {
			t.Fatal(err)
		}
	}
}
