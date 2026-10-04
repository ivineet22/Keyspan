package api_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"store/internal/store"
	"store/internal/worker"
)

func TestCrashContract(t *testing.T) {
	db, shards, srv := start(t)
	var lines []string
	record := func(name, owner string) {
		t.Helper()
		lines = append(lines, fmt.Sprintf("%s owner %s servers 1", name, owner))
		t.Logf("%s owner %s", name, owner)
	}

	t.Run("dest during copy", func(t *testing.T) {
		tenant := "die-dest-copy"
		rg, keys, vals := seedRange(t, srv.URL, db, tenant, names(shards))
		_, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name)
		if err != nil {
			t.Fatal(err)
		}
		stopWorker(t, db, shards, tenant, store.StepSnapshotting)
		shards[1].stop()
		shards[1].start(t)
		if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0]); got != vals[0] {
			t.Fatalf("old owner returned %q", got)
		}
		owner, epoch := rangeOwner(t, db, tenant)
		if owner != rg.Owner || epoch != 1 {
			t.Fatalf("owner %s epoch %d", owner, epoch)
		}
		retrySameValue(t, srv.URL, tenant, keys[0], vals[0])
		assertOneServer(t, shards, tenant, keys[0], epoch, vals[0])
		record("dest_during_copy", owner)
		resumeWorker(t, db, shards, tenant)
		owner, epoch = rangeOwner(t, db, tenant)
		if owner != shards[1].name {
			t.Fatalf("after retry owner %s", owner)
		}
		assertOneServer(t, shards, tenant, keys[0], epoch, vals[0])
	})

	t.Run("source during copy", func(t *testing.T) {
		tenant := "die-source-copy"
		rg, keys, vals := seedRange(t, srv.URL, db, tenant, names(shards))
		_, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name)
		if err != nil {
			t.Fatal(err)
		}
		stopWorker(t, db, shards, tenant, store.StepSnapshotting)
		shards[0].stop()
		status, body := getRaw(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0])
		if status != http.StatusServiceUnavailable || !strings.Contains(body, "range unavailable") {
			t.Fatalf("status %d body %q", status, body)
		}
		owner, epoch := rangeOwner(t, db, tenant)
		if owner != rg.Owner || epoch != 1 {
			t.Fatalf("ownership flipped to %s epoch %d", owner, epoch)
		}
		record("source_during_copy", owner)
		shards[0].start(t)
		if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0]); got != vals[0] {
			t.Fatalf("recovered value %q", got)
		}
		retrySameValue(t, srv.URL, tenant, keys[0], vals[0])
		assertOneServer(t, shards, tenant, keys[0], epoch, vals[0])
	})

	t.Run("source dies while fenced", func(t *testing.T) {
		tenant := "die-source-fence"
		rg, keys, _ := seedRange(t, srv.URL, db, tenant, names(shards))
		_, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name)
		if err != nil {
			t.Fatal(err)
		}
		stopWorker(t, db, shards, tenant, store.StepFenced)
		if status, body := putRaw(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0], "nope"); status != http.StatusServiceUnavailable || !strings.Contains(body, "retry") {
			t.Fatalf("fence status %d body %q", status, body)
		}
		shards[0].stop()
		shards[0].start(t)
		expectFreshSnapshot(t, db, tenant)
		put(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0], "after-fence")
		retrySameValue(t, srv.URL, tenant, keys[0], "after-fence")
		owner, epoch := rangeOwner(t, db, tenant)
		if owner != rg.Owner || epoch != 1 {
			t.Fatalf("owner %s epoch %d", owner, epoch)
		}
		assertOneServer(t, shards, tenant, keys[0], epoch, "after-fence")
		record("fenced_source_dies", owner)
		resumeWorker(t, db, shards, tenant)
		if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0]); got != "after-fence" {
			t.Fatalf("new owner returned %q", got)
		}
		owner, epoch = rangeOwner(t, db, tenant)
		assertOneServer(t, shards, tenant, keys[0], epoch, "after-fence")
	})

	t.Run("dest dies while fenced", func(t *testing.T) {
		tenant := "die-dest-fence"
		rg, keys, _ := seedRange(t, srv.URL, db, tenant, names(shards))
		_, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name)
		if err != nil {
			t.Fatal(err)
		}
		stopWorker(t, db, shards, tenant, store.StepFenced)
		shards[1].stop()
		shards[1].start(t)
		expectFreshSnapshot(t, db, tenant)
		put(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0], "after-fence")
		retrySameValue(t, srv.URL, tenant, keys[0], "after-fence")
		owner, epoch := rangeOwner(t, db, tenant)
		if owner != rg.Owner || epoch != 1 {
			t.Fatalf("owner %s epoch %d", owner, epoch)
		}
		assertOneServer(t, shards, tenant, keys[0], epoch, "after-fence")
		record("fenced_dest_dies", owner)
		resumeWorker(t, db, shards, tenant)
		owner, epoch = rangeOwner(t, db, tenant)
		if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0]); got != "after-fence" {
			t.Fatalf("new owner returned %q", got)
		}
		assertOneServer(t, shards, tenant, keys[0], epoch, "after-fence")
	})

	t.Run("worker after epoch", func(t *testing.T) {
		tenant := "die-worker-epoch"
		rg, keys, vals := seedRange(t, srv.URL, db, tenant, names(shards))
		_, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name)
		if err != nil {
			t.Fatal(err)
		}
		stopWorker(t, db, shards, tenant, store.StepCommitted)
		owner, epoch := rangeOwner(t, db, tenant)
		if owner != shards[1].name || epoch != rg.Epoch+1 {
			t.Fatalf("owner %s epoch %d", owner, epoch)
		}
		resumeWorker(t, db, shards, tenant)
		owner2, epoch2 := rangeOwner(t, db, tenant)
		if owner2 != owner || epoch2 != epoch {
			t.Fatalf("retried commit moved again: owner %s epoch %d", owner2, epoch2)
		}
		retrySameValue(t, srv.URL, tenant, keys[0], vals[0])
		assertOneServer(t, shards, tenant, keys[0], epoch2, vals[0])
		record("worker_after_epoch", owner2)
	})

	t.Run("source after commit", func(t *testing.T) {
		tenant := "die-source-commit"
		rg, keys, vals := seedRange(t, srv.URL, db, tenant, names(shards))
		_, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name)
		if err != nil {
			t.Fatal(err)
		}
		stopWorker(t, db, shards, tenant, store.StepCommitted)
		shards[0].stop()
		if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0]); got != vals[0] {
			t.Fatalf("dest while source is down returned %q", got)
		}
		shards[0].start(t)
		owner, epoch := rangeOwner(t, db, tenant)
		if owner != shards[1].name || epoch != rg.Epoch+1 {
			t.Fatalf("owner %s epoch %d", owner, epoch)
		}
		assertOneServer(t, shards, tenant, keys[0], epoch, vals[0])
		retrySameValue(t, srv.URL, tenant, keys[0], vals[0])
		record("source_after_commit", owner)
		resumeWorker(t, db, shards, tenant)
		assertOneServer(t, shards, tenant, keys[0], epoch, vals[0])
	})

	t.Run("dest after commit", func(t *testing.T) {
		tenant := "die-dest-commit"
		rg, keys, vals := seedRange(t, srv.URL, db, tenant, names(shards))
		_, err := db.InsertMove(context.Background(), tenant, rg.Start, rg.End, rg.Owner, shards[1].name)
		if err != nil {
			t.Fatal(err)
		}
		stopWorker(t, db, shards, tenant, store.StepCommitted)
		shards[1].stop()
		shards[1].start(t)
		if got := get(t, srv.URL+"/v1/tenants/"+tenant+"/keys/"+keys[0]); got != vals[0] {
			t.Fatalf("recovered dest returned %q", got)
		}
		owner, epoch := rangeOwner(t, db, tenant)
		if owner != shards[1].name || epoch != rg.Epoch+1 {
			t.Fatalf("owner %s epoch %d", owner, epoch)
		}
		retrySameValue(t, srv.URL, tenant, keys[0], vals[0])
		assertOneServer(t, shards, tenant, keys[0], epoch, vals[0])
		record("dest_after_commit", owner)
		resumeWorker(t, db, shards, tenant)
	})

	path := filepath.Join("..", "..", "results", "crash.txt")
	body := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if len(lines) != 7 {
		t.Fatalf("recorded %d cases", len(lines))
	}
}

func stopWorker(t *testing.T, db *store.Store, shards []proc, tenant, step string) {
	t.Helper()
	w := &worker.Worker{
		DB: db, Addrs: addrs(shards), ID: "stop-" + tenant,
		Lease: 300 * time.Millisecond, StopAfter: step,
	}
	if err := w.Run(context.Background()); !errors.Is(err, worker.ErrStopped) {
		t.Fatalf("stop at %s: %v", step, err)
	}
}

func resumeWorker(t *testing.T, db *store.Store, shards []proc, tenant string) {
	t.Helper()
	time.Sleep(500 * time.Millisecond)
	w := &worker.Worker{
		DB: db, Addrs: addrs(shards), ID: "resume-" + tenant,
		Lease: 15 * time.Second,
	}
	if err := w.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func rangeOwner(t *testing.T, db *store.Store, tenant string) (string, int64) {
	t.Helper()
	ranges, err := db.EnsureTenant(context.Background(), tenant, []string{"shard-1", "shard-2", "shard-3"})
	if err != nil {
		t.Fatal(err)
	}
	return ranges[0].Owner, ranges[0].Epoch
}

func expectFreshSnapshot(t *testing.T, db *store.Store, tenant string) {
	t.Helper()
	moves, err := db.MovesFor(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 1 {
		t.Fatalf("moves %d", len(moves))
	}
	if moves[0].Step != store.StepSnapshotting {
		t.Fatalf("step %s", moves[0].Step)
	}
	if moves[0].Snapshot != nil || moves[0].Fence != nil {
		t.Fatal("old snapshot was kept")
	}
}

func retrySameValue(t *testing.T, base, tenant, key, value string) {
	t.Helper()
	url := base + "/v1/tenants/" + tenant + "/keys/" + key
	put(t, url, value)
	put(t, url, value)
	if got := get(t, url); got != value {
		t.Fatalf("retry left %q", got)
	}
}

func getRaw(t *testing.T, url string) (int, string) {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(body)
}
