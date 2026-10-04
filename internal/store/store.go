package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"store/internal/hash"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StepPlanned      = "planned"
	StepSnapshotting = "snapshotting"
	StepCatchingUp   = "catching_up"
	StepFenced       = "fenced"
	StepCommitted    = "committed"
	StepCleaning     = "cleaning"
	StepDone         = "done"
	StepAborted      = "aborted"
)

// Range is one half-open span of the hash line, [Start, End), and the shard
// that serves it. Epoch starts at 1 and counts how many times that published
// ownership has changed.
type Range struct {
	Tenant string
	Start  int64
	End    int64
	Owner  string
	Epoch  int64
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	if err := ensureDatabase(ctx, dsn); err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `DROP TABLE IF EXISTS records`); err != nil {
		return fmt.Errorf("drop records: %w", err)
	}
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS ranges (
			tenant_id text NOT NULL,
			hash_start bigint NOT NULL,
			hash_end bigint NOT NULL,
			owner_shard text NOT NULL,
			epoch bigint NOT NULL,
			active_move_id uuid,
			PRIMARY KEY (tenant_id, hash_start),
			CHECK (hash_start < hash_end)
		)`)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS moves (
			id uuid PRIMARY KEY,
			tenant_id text NOT NULL,
			hash_start bigint NOT NULL,
			hash_end bigint NOT NULL,
			source text NOT NULL,
			destination text NOT NULL,
			step text NOT NULL,
			attempt bigint NOT NULL DEFAULT 0,
			lease_owner text,
			lease_until timestamptz,
			snapshot_pos bigint,
			fence_pos bigint,
			applied_pos bigint,
			records_copied bigint NOT NULL DEFAULT 0,
			expected_epoch bigint NOT NULL,
			CHECK (hash_start < hash_end),
			CHECK (step IN ('planned', 'snapshotting', 'catching_up', 'fenced', 'committed', 'cleaning', 'done', 'aborted'))
		)`)
	if err != nil {
		return fmt.Errorf("migrate moves: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE moves ADD COLUMN IF NOT EXISTS applied_pos bigint`); err != nil {
		return fmt.Errorf("migrate applied_pos: %w", err)
	}
	// Older databases have a check that does not list catching_up. Replace it.
	if _, err := s.pool.Exec(ctx, `
		DO $$
		DECLARE r record;
		BEGIN
			FOR r IN
				SELECT conname FROM pg_constraint
				WHERE conrelid = 'moves'::regclass AND contype = 'c'
				  AND pg_get_constraintdef(oid) LIKE '%step%'
			LOOP
				EXECUTE format('ALTER TABLE moves DROP CONSTRAINT %I', r.conname);
			END LOOP;
		END $$`); err != nil {
		return fmt.Errorf("drop step check: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
		ALTER TABLE moves ADD CONSTRAINT moves_step_check
		CHECK (step IN ('planned', 'snapshotting', 'catching_up', 'fenced', 'committed', 'cleaning', 'done', 'aborted'))`); err != nil {
		return fmt.Errorf("step check: %w", err)
	}
	return nil
}

// Move is one copy job for a single range. Step is how far that job got.
// ExpectedEpoch is the epoch the source still owned when the job was inserted.
type Move struct {
	ID            string
	Tenant        string
	Start         int64
	End           int64
	Source        string
	Destination   string
	Step          string
	Attempt       int64
	Snapshot      *uint64
	Fence         *uint64
	Applied       *uint64
	RecordsCopied int64
	ExpectedEpoch int64
}

// Placement is one range plus whether a move job already holds it.
type Placement struct {
	Range
	Busy bool
}

// RemoveAllRanges deletes ownership rows and move jobs so a test starts empty.
func (s *Store) RemoveAllRanges(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE moves, ranges`)
	return err
}

// InsertMove records one job and marks the range busy in the same transaction.
// A range that already has a job is left unchanged and returns an error.
func (s *Store) InsertMove(ctx context.Context, tenant string, start, end int64, source, destination string) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var owner string
	var epoch int64
	err = tx.QueryRow(ctx, `
		SELECT owner_shard, epoch FROM ranges
		WHERE tenant_id = $1 AND hash_start = $2
		FOR UPDATE`, tenant, start).Scan(&owner, &epoch)
	if err != nil {
		return "", fmt.Errorf("lock range: %w", err)
	}
	if owner != source {
		return "", fmt.Errorf("range owner is %s", owner)
	}
	var id string
	err = tx.QueryRow(ctx, `
		INSERT INTO moves (
			id, tenant_id, hash_start, hash_end, source, destination, step, expected_epoch
		) VALUES (
			gen_random_uuid(), $1, $2, $3, $4, $5, 'planned', $6
		) RETURNING id::text`,
		tenant, start, end, source, destination, epoch).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("insert move: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE ranges SET active_move_id = $1::uuid
		WHERE tenant_id = $2 AND hash_start = $3 AND active_move_id IS NULL`,
		id, tenant, start)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() != 1 {
		return "", fmt.Errorf("range already has a move")
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return id, nil
}

// Lease claims one unfinished job. The same worker may renew its own lease.
// A different worker waits until lease_until has passed.
func (s *Store) Lease(ctx context.Context, owner string, forDur time.Duration) (*Move, error) {
	ms := forDur.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	var m Move
	var snap, fence, applied sql.NullInt64
	err := s.pool.QueryRow(ctx, `
		UPDATE moves AS m
		SET lease_owner = $1,
		    lease_until = now() + ($2::bigint * interval '1 millisecond'),
		    attempt = attempt + 1
		WHERE m.id = (
			SELECT id FROM moves
			WHERE step NOT IN ('done', 'aborted')
			  AND (lease_until IS NULL OR lease_until < now() OR lease_owner = $1)
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING m.id::text, m.tenant_id, m.hash_start, m.hash_end, m.source, m.destination,
		          m.step, m.attempt, m.snapshot_pos, m.fence_pos, m.applied_pos, m.records_copied, m.expected_epoch`,
		owner, ms).Scan(
		&m.ID, &m.Tenant, &m.Start, &m.End, &m.Source, &m.Destination,
		&m.Step, &m.Attempt, &snap, &fence, &applied, &m.RecordsCopied, &m.ExpectedEpoch)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	m.Snapshot = uintPtr(snap)
	m.Fence = uintPtr(fence)
	m.Applied = uintPtr(applied)
	return &m, nil
}

// NoteProgress writes the positions and copy count for the step the worker holds.
// applied is how far the destination has copied and synced. It moves only forward.
func (s *Store) NoteProgress(ctx context.Context, id, owner, step string, snapshot, fence, applied *uint64, copied *int64) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE moves
		SET snapshot_pos = COALESCE($4, snapshot_pos),
		    fence_pos = COALESCE($5, fence_pos),
		    applied_pos = CASE
		        WHEN $7::bigint IS NULL THEN applied_pos
		        WHEN applied_pos IS NULL OR $7::bigint > applied_pos THEN $7::bigint
		        ELSE applied_pos
		    END,
		    records_copied = COALESCE($6, records_copied)
		WHERE id = $1::uuid AND lease_owner = $2 AND step = $3`,
		id, owner, step, int64Ptr(snapshot), int64Ptr(fence), copied, int64Ptr(applied))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("progress lost the lease")
	}
	return nil
}

// Advance moves the job to the next step while this worker still holds it.
func (s *Store) Advance(ctx context.Context, id, owner, from, to string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE moves SET step = $4
		WHERE id = $1::uuid AND lease_owner = $2 AND step = $3`,
		id, owner, from, to)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("advance lost the lease")
	}
	return nil
}

// CommitOwnership publishes the destination as the owner if the epoch is still
// the one this job started from. A retry that sees the new owner is success.
func (s *Store) CommitOwnership(ctx context.Context, m *Move) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE ranges
		SET owner_shard = $1, epoch = epoch + 1
		WHERE tenant_id = $2 AND hash_start = $3 AND owner_shard = $4 AND epoch = $5`,
		m.Destination, m.Tenant, m.Start, m.Source, m.ExpectedEpoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var owner string
	var epoch int64
	err = s.pool.QueryRow(ctx, `
		SELECT owner_shard, epoch FROM ranges
		WHERE tenant_id = $1 AND hash_start = $2`, m.Tenant, m.Start).Scan(&owner, &epoch)
	if err != nil {
		return err
	}
	if owner == m.Destination && epoch == m.ExpectedEpoch+1 {
		return nil
	}
	return fmt.Errorf("commit conflict: owner %s epoch %d", owner, epoch)
}

// Abort cancels the job only while the source is still the owner at the epoch
// the job started from. If the epoch already advanced, the abort does nothing
// and returns an error. The destination stays the owner.
func (s *Store) Abort(ctx context.Context, id string) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var tenant, source, step string
	var start, expected int64
	err = tx.QueryRow(ctx, `
		SELECT tenant_id, hash_start, source, step, expected_epoch
		FROM moves WHERE id = $1::uuid FOR UPDATE`, id).Scan(&tenant, &start, &source, &step, &expected)
	if err != nil {
		return "", err
	}
	if step == StepDone || step == StepAborted {
		return "", fmt.Errorf("abort lost: step is %s", step)
	}
	var owner string
	var epoch int64
	err = tx.QueryRow(ctx, `
		SELECT owner_shard, epoch FROM ranges
		WHERE tenant_id = $1 AND hash_start = $2
		FOR UPDATE`, tenant, start).Scan(&owner, &epoch)
	if err != nil {
		return "", err
	}
	if owner != source || epoch != expected {
		return "", fmt.Errorf("abort lost: owner %s epoch %d", owner, epoch)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE moves SET step = 'aborted'
		WHERE id = $1::uuid AND step = $2`, id, step)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() != 1 {
		return "", fmt.Errorf("abort lost the row")
	}
	if _, err := tx.Exec(ctx, `
		UPDATE ranges SET active_move_id = NULL
		WHERE active_move_id = $1::uuid`, id); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return source, nil
}

// Split cuts one span at its midpoint. Both halves stay with the same owner.
// The epoch on each half is one higher than the span that was cut. The span
// must not have a move in progress. Splitting does not copy any values.
func (s *Store) Split(ctx context.Context, tenant string, start int64) (left, right Range, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Range{}, Range{}, err
	}
	defer tx.Rollback(ctx)
	var owner string
	var end, epoch int64
	var active sql.NullString
	err = tx.QueryRow(ctx, `
		SELECT hash_end, owner_shard, epoch, active_move_id::text
		FROM ranges
		WHERE tenant_id = $1 AND hash_start = $2
		FOR UPDATE`, tenant, start).Scan(&end, &owner, &epoch, &active)
	if err != nil {
		return Range{}, Range{}, fmt.Errorf("lock range: %w", err)
	}
	if active.Valid {
		return Range{}, Range{}, fmt.Errorf("range has a move in progress")
	}
	if end-start < 2 {
		return Range{}, Range{}, fmt.Errorf("range is too small to split")
	}
	mid := start + (end-start)/2
	if mid <= start || mid >= end {
		return Range{}, Range{}, fmt.Errorf("midpoint %d is outside [%d, %d)", mid, start, end)
	}
	next := epoch + 1
	tag, err := tx.Exec(ctx, `
		UPDATE ranges SET hash_end = $3, epoch = $4
		WHERE tenant_id = $1 AND hash_start = $2 AND active_move_id IS NULL AND epoch = $5`,
		tenant, start, mid, next, epoch)
	if err != nil {
		return Range{}, Range{}, err
	}
	if tag.RowsAffected() != 1 {
		return Range{}, Range{}, fmt.Errorf("split lost the range")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO ranges (tenant_id, hash_start, hash_end, owner_shard, epoch, active_move_id)
		VALUES ($1, $2, $3, $4, $5, NULL)`,
		tenant, mid, end, owner, next); err != nil {
		return Range{}, Range{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Range{}, Range{}, err
	}
	left = Range{Tenant: tenant, Start: start, End: mid, Owner: owner, Epoch: next}
	right = Range{Tenant: tenant, Start: mid, End: end, Owner: owner, Epoch: next}
	return left, right, nil
}

// Placements returns every span and whether a move job holds it.
func (s *Store) Placements(ctx context.Context) ([]Placement, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id, hash_start, hash_end, owner_shard, epoch, active_move_id IS NOT NULL
		FROM ranges
		ORDER BY tenant_id, hash_start`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Placement
	for rows.Next() {
		var p Placement
		if err := rows.Scan(&p.Tenant, &p.Start, &p.End, &p.Owner, &p.Epoch, &p.Busy); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// RecordsCopied sums the records_copied column of finished jobs.
func (s *Store) RecordsCopied(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(records_copied), 0) FROM moves WHERE step = 'done'`).Scan(&n)
	return n, err
}

// InsertRange adds one span. It refuses a span that overlaps a span this tenant already has.
func (s *Store) InsertRange(ctx context.Context, tenant, owner string, start, end, epoch int64) error {
	if start >= end {
		return fmt.Errorf("empty span")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, tenant); err != nil {
		return err
	}
	ranges, err := listRanges(ctx, tx, tenant)
	if err != nil {
		return err
	}
	for _, rg := range ranges {
		if start < rg.End && end > rg.Start {
			return fmt.Errorf("range overlaps [%d, %d)", rg.Start, rg.End)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO ranges (tenant_id, hash_start, hash_end, owner_shard, epoch, active_move_id)
		VALUES ($1, $2, $3, $4, $5, NULL)`,
		tenant, start, end, owner, epoch); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Finish marks the job done and clears the range's active move in one transaction.
func (s *Store) Finish(ctx context.Context, id, owner string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		UPDATE moves SET step = 'done'
		WHERE id = $1::uuid AND lease_owner = $2 AND step = 'cleaning'`, id, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("finish lost the lease")
	}
	if _, err := tx.Exec(ctx, `
		UPDATE ranges SET active_move_id = NULL
		WHERE active_move_id = $1::uuid`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// MovesFor returns every job for a tenant, oldest id first.
func (s *Store) MovesFor(ctx context.Context, tenant string) ([]Move, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, tenant_id, hash_start, hash_end, source, destination,
		       step, attempt, snapshot_pos, fence_pos, applied_pos, records_copied, expected_epoch
		FROM moves
		WHERE tenant_id = $1
		ORDER BY id`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Move
	for rows.Next() {
		var m Move
		var snap, fence, applied sql.NullInt64
		if err := rows.Scan(
			&m.ID, &m.Tenant, &m.Start, &m.End, &m.Source, &m.Destination,
			&m.Step, &m.Attempt, &snap, &fence, &applied, &m.RecordsCopied, &m.ExpectedEpoch); err != nil {
			return nil, err
		}
		m.Snapshot = uintPtr(snap)
		m.Fence = uintPtr(fence)
		m.Applied = uintPtr(applied)
		out = append(out, m)
	}
	return out, rows.Err()
}

func uintPtr(v sql.NullInt64) *uint64 {
	if !v.Valid || v.Int64 < 0 {
		return nil
	}
	n := uint64(v.Int64)
	return &n
}

func int64Ptr(v *uint64) *int64 {
	if v == nil {
		return nil
	}
	n := int64(*v)
	return &n
}

// RecordsGone reports whether the Phase 1 value table has been removed.
func (s *Store) RecordsGone(ctx context.Context) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT to_regclass('public.records') IS NOT NULL`).Scan(&exists)
	return !exists, err
}

// EnsureTenant returns the spans for a tenant. The first caller creates one
// equal span per owner, at epoch 1, with no move in progress.
func (s *Store) EnsureTenant(ctx context.Context, tenant string, owners []string) ([]Range, error) {
	if len(owners) == 0 {
		return nil, fmt.Errorf("no shards configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, tenant); err != nil {
		return nil, fmt.Errorf("lock tenant: %w", err)
	}
	ranges, err := listRanges(ctx, tx, tenant)
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		parts := hash.Parts(len(owners))
		for i, part := range parts {
			if _, err := tx.Exec(ctx, `
				INSERT INTO ranges (tenant_id, hash_start, hash_end, owner_shard, epoch, active_move_id)
				VALUES ($1, $2, $3, $4, 1, NULL)`,
				tenant, part[0], part[1], owners[i]); err != nil {
				return nil, fmt.Errorf("seed range: %w", err)
			}
		}
		ranges, err = listRanges(ctx, tx, tenant)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return ranges, nil
}

type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func listRanges(ctx context.Context, q queryer, tenant string) ([]Range, error) {
	rows, err := q.Query(ctx, `
		SELECT tenant_id, hash_start, hash_end, owner_shard, epoch
		FROM ranges
		WHERE tenant_id = $1
		ORDER BY hash_start`, tenant)
	if err != nil {
		return nil, fmt.Errorf("list ranges: %w", err)
	}
	defer rows.Close()
	var out []Range
	for rows.Next() {
		var rg Range
		if err := rows.Scan(&rg.Tenant, &rg.Start, &rg.End, &rg.Owner, &rg.Epoch); err != nil {
			return nil, err
		}
		out = append(out, rg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func ensureDatabase(ctx context.Context, dsn string) error {
	u, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	name := strings.TrimPrefix(u.Path, "/")
	ident, err := quoteIdent(name)
	if err != nil {
		return err
	}
	u.Path = "/postgres"
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		return fmt.Errorf("connect admin: %w", err)
	}
	defer conn.Close(ctx)

	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return fmt.Errorf("check database: %w", err)
	}
	if exists {
		return nil
	}
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+ident); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	return nil
}

func quoteIdent(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("empty database name")
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return "", fmt.Errorf("invalid database name %q", name)
		}
	}
	return `"` + name + `"`, nil
}
