package api_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"store/internal/hash"
	"store/internal/store"
	"store/internal/wire"
	"store/internal/worker"
)

func TestPutsDuringCopyAreReadableAfterCommit(t *testing.T) {
	db, shards, srv := start(t)
	tenant := "live"
	rg, keys, vals := seedRange(t, srv.URL, db, tenant, names(shards))
	if _, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name); err != nil {
		t.Fatal(err)
	}
	stop := func(id, step string) {
		t.Helper()
		w := &worker.Worker{
			DB: db, Addrs: addrs(shards), ID: id, Lease: 300 * time.Millisecond,
			StopAfter: step, Tail: 1,
		}
		if err := w.Run(context.Background()); !errors.Is(err, worker.ErrStopped) {
			t.Fatalf("stop at %s: %v", step, err)
		}
	}
	stop("snap", store.StepSnapshotting)
	extra := make([]string, 3)
	skip := map[string]struct{}{}
	for i := range extra {
		extra[i] = keyInSpan(t, tenant, rg, skip)
		skip[extra[i]] = struct{}{}
		put(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+extra[i], "during-snap-"+extra[i])
	}
	time.Sleep(500 * time.Millisecond)
	stop("catch", store.StepCatchingUp)
	moves, err := db.MovesFor(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	if moves[0].Step != store.StepCatchingUp || moves[0].Applied == nil || moves[0].Snapshot == nil || *moves[0].Applied <= *moves[0].Snapshot {
		t.Fatalf("catch-up did not apply past the snapshot: %+v", moves[0])
	}
	put(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0], "after-catchup")
	time.Sleep(500 * time.Millisecond)
	stop("fence", store.StepFenced)
	status, body := putRaw(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0], "during-fence")
	if status != http.StatusServiceUnavailable || !strings.Contains(body, "retry") {
		t.Fatalf("fenced put status %d body %q", status, body)
	}
	if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0]); got != "after-catchup" {
		t.Fatalf("fence changed the value to %q", got)
	}
	time.Sleep(500 * time.Millisecond)
	done := &worker.Worker{DB: db, Addrs: addrs(shards), ID: "finish", Lease: 15 * time.Second, Tail: 1}
	if err := done.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{keys[0]: "after-catchup"}
	for i := 1; i < len(keys); i++ {
		want[keys[i]] = vals[i]
	}
	for _, key := range extra {
		want[key] = "during-snap-" + key
	}
	for key, val := range want {
		if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+key); got != val {
			t.Fatalf("get %s = %q, want %q", key, got, val)
		}
		assertOneServer(t, shards, tenant, key, 2, val)
	}
	moves, err = db.MovesFor(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 1 || moves[0].Step != store.StepDone {
		t.Fatalf("moves %+v", moves)
	}
	t.Logf("records_copied=%d", moves[0].RecordsCopied)
}

func TestAbortLosesOnceEpochAdvances(t *testing.T) {
	db, shards, srv := start(t)
	tenant := "abort-late"
	rg, keys, vals := seedRange(t, srv.URL, db, tenant, names(shards))
	id, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name)
	if err != nil {
		t.Fatal(err)
	}
	w := &worker.Worker{
		DB: db, Addrs: addrs(shards), ID: "late", Lease: 300 * time.Millisecond,
		StopAfter: store.StepCommitted,
	}
	if err := w.Run(context.Background()); !errors.Is(err, worker.ErrStopped) {
		t.Fatal(err)
	}
	if err := w.Abort(context.Background(), id); err == nil {
		t.Fatal("abort succeeded after the epoch advanced")
	}
	ranges, err := db.EnsureTenant(context.Background(), tenant, names(shards))
	if err != nil {
		t.Fatal(err)
	}
	if ranges[0].Owner != shards[1].name || ranges[0].Epoch != 2 {
		t.Fatalf("owner %s epoch %d", ranges[0].Owner, ranges[0].Epoch)
	}
	time.Sleep(500 * time.Millisecond)
	finish := &worker.Worker{DB: db, Addrs: addrs(shards), ID: "late-finish", Lease: 15 * time.Second}
	if err := finish.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i, key := range keys {
		if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+key); got != vals[i] {
			t.Fatalf("get %s = %q", key, got)
		}
		assertOneServer(t, shards, tenant, key, 2, vals[i])
	}
}

func TestAbortBeforeCommitLetsTheSourceServe(t *testing.T) {
	db, shards, srv := start(t)
	tenant := "abort-early"
	rg, keys, _ := seedRange(t, srv.URL, db, tenant, names(shards))
	id, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name)
	if err != nil {
		t.Fatal(err)
	}
	w := &worker.Worker{
		DB: db, Addrs: addrs(shards), ID: "early", Lease: 300 * time.Millisecond,
		StopAfter: store.StepFenced,
	}
	if err := w.Run(context.Background()); !errors.Is(err, worker.ErrStopped) {
		t.Fatal(err)
	}
	if status, _ := putRaw(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0], "blocked"); status != 503 {
		t.Fatalf("put during fence status %d", status)
	}
	if err := w.Abort(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	put(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0], "after-abort")
	idle := &worker.Worker{DB: db, Addrs: addrs(shards), ID: "after-abort", Lease: 15 * time.Second}
	if err := idle.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	ranges, err := db.EnsureTenant(context.Background(), tenant, names(shards))
	if err != nil {
		t.Fatal(err)
	}
	if ranges[0].Owner != rg.Owner || ranges[0].Epoch != 1 {
		t.Fatalf("owner %s epoch %d", ranges[0].Owner, ranges[0].Epoch)
	}
	moves, err := db.MovesFor(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 1 || moves[0].Step != store.StepAborted {
		t.Fatalf("moves %+v", moves)
	}
	if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0]); got != "after-abort" {
		t.Fatalf("get %q", got)
	}
	assertOneServer(t, shards, tenant, keys[0], 1, "after-abort")
}

func TestRangesRejectOverlap(t *testing.T) {
	db, shards, _ := start(t)
	tenant := "cover"
	ranges, err := db.EnsureTenant(context.Background(), tenant, names(shards))
	if err != nil {
		t.Fatal(err)
	}
	if ranges[0].Start != 0 || ranges[len(ranges)-1].End != hash.Space {
		t.Fatalf("coverage [%d, %d)", ranges[0].Start, ranges[len(ranges)-1].End)
	}
	for i := 1; i < len(ranges); i++ {
		if ranges[i].Start != ranges[i-1].End {
			t.Fatalf("span %d starts at %d, previous ends at %d", i, ranges[i].Start, ranges[i-1].End)
		}
	}
	err = db.InsertRange(context.Background(), tenant, shards[0].name, ranges[0].Start, ranges[0].Start+10, 1)
	if err == nil {
		t.Fatal("overlapping range was accepted")
	}
}

func keyInSpan(t *testing.T, tenant string, rg store.Range, skip map[string]struct{}) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("n%d", i)
		if skip != nil {
			if _, ok := skip[key]; ok {
				continue
			}
		}
		place := int64(hash.Key(tenant, key))
		if place >= rg.Start && place < rg.End {
			return key
		}
	}
	t.Fatal("no key in span")
	return ""
}

func assertOneServer(t *testing.T, shards []proc, tenant, key string, epoch int64, want string) {
	t.Helper()
	served := 0
	for _, p := range shards {
		got, err := wire.Get(context.Background(), p.addr, tenant, key, epoch)
		if err != nil {
			continue
		}
		served++
		if string(got) != want {
			t.Fatalf("%s served %q", p.name, got)
		}
	}
	if served != 1 {
		t.Fatalf("%s/%s served by %d shards at epoch %d", tenant, key, served, epoch)
	}
}
