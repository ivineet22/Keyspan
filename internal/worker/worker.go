// Package worker runs one move job at a time. It does not store the record
// bytes. The destination shard pulls them from the source.
package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"store/internal/store"
	"store/internal/wire"
)

// ErrStopped means the worker finished the body of StopAfter and left the job
// on that step. The lease expires on its own so another worker can continue.
var ErrStopped = errors.New("worker stopped")

// errNotCaughtUp means the destination is still behind by more than the tail.
// The same step runs again. The job does not advance.
var errNotCaughtUp = errors.New("still catching up")

// TailLimit is how many log positions may still be uncopied when the source
// is fenced. A position is one record in the source log. The fence then copies
// that short remainder. Zero on a Worker means this default.
const TailLimit uint64 = 8

// Worker leases one row from Postgres and runs a single step, then loops.
// StopAfter, when set, returns after that step's body and does not advance.
type Worker struct {
	DB        *store.Store
	Addrs     map[string]string
	ID        string
	Lease     time.Duration
	StopAfter string
	Tail      uint64
	// CopyCap is how many records this run may copy before it leaves the rest
	// of the queue for the next run. Zero means no cap. The current job is
	// finished; the cap stops the run from leasing another one.
	CopyCap int64
	Copied  int64
}

func (w *Worker) Run(ctx context.Context) error {
	id := w.ID
	if id == "" {
		id = "worker"
	}
	lease := w.Lease
	if lease <= 0 {
		lease = 15 * time.Second
	}
	for {
		if w.CopyCap > 0 && w.Copied >= w.CopyCap {
			return nil
		}
		m, err := w.DB.Lease(ctx, id, lease)
		if err != nil {
			return err
		}
		if m == nil {
			return nil
		}
		err = w.step(ctx, id, m)
		if errors.Is(err, errNotCaughtUp) {
			if w.StopAfter == m.Step {
				return ErrStopped
			}
			continue
		}
		if err != nil {
			return err
		}
		if w.StopAfter == m.Step {
			return ErrStopped
		}
		next, err := nextStep(m.Step)
		if err != nil {
			return err
		}
		if next == store.StepDone {
			if err := w.DB.Finish(ctx, m.ID, id); err != nil {
				return err
			}
			w.Copied += m.RecordsCopied
			continue
		}
		if err := w.DB.Advance(ctx, m.ID, id, m.Step, next); err != nil {
			return err
		}
	}
}

func nextStep(step string) (string, error) {
	switch step {
	case store.StepPlanned:
		return store.StepSnapshotting, nil
	case store.StepSnapshotting:
		return store.StepCatchingUp, nil
	case store.StepCatchingUp:
		return store.StepFenced, nil
	case store.StepFenced:
		return store.StepCommitted, nil
	case store.StepCommitted:
		return store.StepCleaning, nil
	case store.StepCleaning:
		return store.StepDone, nil
	default:
		return "", fmt.Errorf("no next step after %s", step)
	}
}

func (w *Worker) step(ctx context.Context, id string, m *store.Move) error {
	switch m.Step {
	case store.StepPlanned:
		return nil
	case store.StepSnapshotting:
		return w.snapshot(ctx, id, m)
	case store.StepCatchingUp:
		return w.catchUp(ctx, id, m)
	case store.StepFenced:
		return w.fence(ctx, id, m)
	case store.StepCommitted:
		return w.commit(ctx, m)
	case store.StepCleaning:
		return w.clean(ctx, m)
	default:
		return fmt.Errorf("unknown step %s", m.Step)
	}
}

func (w *Worker) snapshot(ctx context.Context, id string, m *store.Move) error {
	pos := m.Snapshot
	if pos == nil {
		got, err := wire.Sync(ctx, w.Addrs[m.Source])
		if err != nil {
			return err
		}
		pos = &got
	}
	copied := int64(0)
	if *pos > 0 {
		n, err := w.pull(ctx, m, 0, *pos)
		if err != nil {
			return err
		}
		copied = int64(n)
	}
	if err := w.DB.NoteProgress(ctx, m.ID, id, m.Step, pos, nil, nil, &copied); err != nil {
		return err
	}
	m.Snapshot = pos
	m.RecordsCopied = copied
	return nil
}

func (w *Worker) fence(ctx context.Context, id string, m *store.Move) error {
	if err := wire.Fence(ctx, w.Addrs[m.Source], m.Tenant, m.Start, m.End, m.ExpectedEpoch); err != nil {
		return err
	}
	pos := m.Fence
	if pos == nil {
		got, err := wire.Sync(ctx, w.Addrs[m.Source])
		if err != nil {
			return err
		}
		pos = &got
	}
	after := uint64(0)
	if m.Applied != nil {
		after = *m.Applied
	} else if m.Snapshot != nil {
		after = *m.Snapshot
	}
	copied := m.RecordsCopied
	if *pos > 0 {
		if *pos < after {
			return fmt.Errorf("fence behind the copied cursor")
		}
		n, err := w.pull(ctx, m, after, *pos)
		if err != nil {
			return err
		}
		copied = int64(n)
	}
	if err := w.DB.NoteProgress(ctx, m.ID, id, m.Step, nil, pos, nil, &copied); err != nil {
		return err
	}
	m.Fence = pos
	m.RecordsCopied = copied
	return nil
}

func (w *Worker) commit(ctx context.Context, m *store.Move) error {
	if err := w.DB.CommitOwnership(ctx, m); err != nil {
		return err
	}
	return wire.Install(ctx, w.Addrs[m.Destination], m.Tenant, m.ID, m.Start, m.End)
}

func (w *Worker) clean(ctx context.Context, m *store.Move) error {
	return wire.DropRange(ctx, w.Addrs[m.Source], m.Tenant, m.Start, m.End, m.ExpectedEpoch+1)
}

// catchUp copies records written after the snapshot while the source is still
// serving. It stops copying once the remainder is within the tail, and the
// fence step takes that remainder. A bigger gap is synced and the step repeats.
func (w *Worker) catchUp(ctx context.Context, id string, m *store.Move) error {
	if m.Snapshot == nil {
		return fmt.Errorf("catch-up without a snapshot")
	}
	applied := *m.Snapshot
	if m.Applied != nil && *m.Applied > applied {
		applied = *m.Applied
	}
	sourcePos, err := wire.Sync(ctx, w.Addrs[m.Source])
	if err != nil {
		return err
	}
	if sourcePos < applied {
		return fmt.Errorf("source log shrank")
	}
	if sourcePos-applied <= w.tail() {
		return nil
	}
	through := sourcePos - w.tail()
	n, err := w.pull(ctx, m, applied, through)
	if err != nil {
		return err
	}
	copied := int64(n)
	if err := w.DB.NoteProgress(ctx, m.ID, id, m.Step, nil, nil, &through, &copied); err != nil {
		return err
	}
	m.Applied = &through
	m.RecordsCopied = copied
	return errNotCaughtUp
}

// Abort cancels the job if the epoch has not moved, then tells the source to
// drop its fence. If the epoch already advanced, the fence stays and the
// destination remains the owner.
func (w *Worker) Abort(ctx context.Context, id string) error {
	source, err := w.DB.Abort(ctx, id)
	if err != nil {
		return err
	}
	return wire.Observe(ctx, w.Addrs[source])
}

func (w *Worker) tail() uint64 {
	if w.Tail == 0 {
		return TailLimit
	}
	return w.Tail
}

// pull copies through the given position and refuses to treat that as done
// unless the destination reports the position was synced.
func (w *Worker) pull(ctx context.Context, m *store.Move, after, through uint64) (uint64, error) {
	n, durable, err := wire.Pull(ctx, w.Addrs[m.Destination], w.Addrs[m.Source], m.Tenant, m.Start, m.End, after, through, m.ExpectedEpoch, m.ID)
	if err != nil {
		return 0, err
	}
	if err := acceptCursor(durable, through); err != nil {
		return 0, err
	}
	return n, nil
}

func acceptCursor(durable, through uint64) error {
	if through > 0 && durable < through {
		return fmt.Errorf("ack without sync")
	}
	return nil
}
