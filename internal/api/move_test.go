package api_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"store/internal/hash"
	"store/internal/store"
	"store/internal/worker"
)

func TestMoveResumesAfterEachStep(t *testing.T) {
	db, shards, srv := start(t)
	steps := []string{
		store.StepPlanned,
		store.StepSnapshotting,
		store.StepCatchingUp,
		store.StepFenced,
		store.StepCommitted,
		store.StepCleaning,
	}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			tenant := "mv-" + step
			rg, keys, vals := seedRange(t, srv.URL, db, tenant, names(shards))
			id, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name)
			if err != nil {
				t.Fatal(err)
			}
			stopped := &worker.Worker{
				DB:        db,
				Addrs:     addrs(shards),
				ID:        "stop-" + step,
				Lease:     300 * time.Millisecond,
				StopAfter: step,
			}
			err = stopped.Run(context.Background())
			if !errors.Is(err, worker.ErrStopped) {
				t.Fatalf("stop at %s: %v", step, err)
			}
			if step == store.StepFenced {
				status, body := putRaw(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0], "nope")
				if status != http.StatusServiceUnavailable || !strings.Contains(body, "retry") {
					t.Fatalf("fenced put status %d body %q", status, body)
				}
				if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0]); got != vals[0] {
					t.Fatalf("fenced get %q", got)
				}
			}
			time.Sleep(500 * time.Millisecond)
			resumed := &worker.Worker{
				DB:    db,
				Addrs: addrs(shards),
				ID:    "resume-" + step,
				Lease: 15 * time.Second,
			}
			if err := resumed.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			assertMoved(t, db, srv.URL, tenant, shards[1].name, id, keys, vals)
		})
	}
}

func TestDestCrashDuringSnapshot(t *testing.T) {
	db, shards, srv := start(t)
	tenant := "crash"
	rg, keys, vals := seedRange(t, srv.URL, db, tenant, names(shards))
	if _, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name); err != nil {
		t.Fatal(err)
	}
	stopped := &worker.Worker{
		DB:        db,
		Addrs:     addrs(shards),
		ID:        "crash-stop",
		Lease:     300 * time.Millisecond,
		StopAfter: store.StepSnapshotting,
	}
	if err := stopped.Run(context.Background()); !errors.Is(err, worker.ErrStopped) {
		t.Fatal(err)
	}
	pending := filepath.Join(shards[1].dir, "pending_*.bin")
	matches, err := filepath.Glob(pending)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("destination has no pending copy to discard")
	}
	shards[1].stop()
	shards[1].start(t)
	matches, err = filepath.Glob(pending)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("pending copy survived restart: %v", matches)
	}
	for i, key := range keys {
		if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+key); got != vals[i] {
			t.Fatalf("old owner get %s = %q", key, got)
		}
	}
	time.Sleep(500 * time.Millisecond)
	resumed := &worker.Worker{
		DB:    db,
		Addrs: addrs(shards),
		ID:    "crash-resume",
		Lease: 15 * time.Second,
	}
	if err := resumed.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertMoved(t, db, srv.URL, tenant, shards[1].name, "", keys, vals)
}

func seedRange(t *testing.T, base string, db *store.Store, tenant string, owners []string) (store.Range, []string, []string) {
	t.Helper()
	ranges, err := db.EnsureTenant(context.Background(), tenant, owners)
	if err != nil {
		t.Fatal(err)
	}
	rg := ranges[0]
	var keys, vals []string
	for i := 0; len(keys) < 4 && i < 100000; i++ {
		key := fmt.Sprintf("k%d", i)
		place := int64(hash.Key(tenant, key))
		if place < rg.Start || place >= rg.End {
			continue
		}
		val := "v-" + key
		put(t, base+"/v1/tenants/"+tenant+"/keys/"+key, val)
		keys = append(keys, key)
		vals = append(vals, val)
	}
	if len(keys) != 4 {
		t.Fatal("could not place 4 keys in the first span")
	}
	if rg.Owner != owners[0] {
		t.Fatalf("first span owner %s", rg.Owner)
	}
	return rg, keys, vals
}

func assertMoved(t *testing.T, db *store.Store, base, tenant, dest, id string, keys, vals []string) {
	t.Helper()
	ranges, err := db.EnsureTenant(context.Background(), tenant, []string{"shard-1", "shard-2", "shard-3"})
	if err != nil {
		t.Fatal(err)
	}
	if ranges[0].Owner != dest {
		t.Fatalf("owner %s", ranges[0].Owner)
	}
	if ranges[0].Epoch != 2 {
		t.Fatalf("epoch %d", ranges[0].Epoch)
	}
	for i, key := range keys {
		if got := get(t, base+"/v1/tenants/"+tenant+"/keys/"+key); got != vals[i] {
			t.Fatalf("get %s = %q", key, got)
		}
	}
	moves, err := db.MovesFor(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 1 {
		t.Fatalf("moves = %d", len(moves))
	}
	if moves[0].Step != store.StepDone {
		t.Fatalf("step %s", moves[0].Step)
	}
	if id != "" && moves[0].ID != id {
		t.Fatalf("move id %s", moves[0].ID)
	}
	if moves[0].RecordsCopied < 1 {
		t.Fatalf("records_copied %d", moves[0].RecordsCopied)
	}
	t.Logf("records_copied=%d", moves[0].RecordsCopied)
}

func addrs(shards []proc) map[string]string {
	out := make(map[string]string, len(shards))
	for _, p := range shards {
		out[p.name] = p.addr
	}
	return out
}

func putRaw(t *testing.T, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	got, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(got)
}
