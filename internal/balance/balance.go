// Package balance decides which slice to move. It inserts at most one move
// job per pass. The worker still does the copy.
package balance

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"store/internal/store"

	"github.com/redis/go-redis/v9"
)

// Threshold is how far above the average a shard must be before a pass acts.
// 1.5 means the busiest shard has one and a half times the average shard's requests.
const Threshold = 1.5

// Action is one pass. Op is "none", "move", or "split".
// A split also enqueues a move of the hotter half.
type Action struct {
	Op          string
	Tenant      string
	Start       int64
	End         int64
	Source      string
	Destination string
	MoveID      string
}

// Balancer reads Redis rates and the range rows, then either does nothing,
// inserts one move, or splits a slice and inserts one move of the hotter half.
type Balancer struct {
	DB    *store.Store
	Redis *redis.Client
	// Owners are the shard names, in a stable order. The average is taken over
	// this list, so a shard with no traffic still counts.
	Owners []string
	// CopyCap is the most records a pass should copy. Zero means one move is
	// the whole cap. A pass that has already copied this many inserts nothing.
	CopyCap int64
	Splits  int
	seen    map[string]int64
}

func rangeKey(tenant string, start int64) string {
	return tenant + "/" + strconv.FormatInt(start, 10)
}

// Observe records the epoch of every span. Plan will not enqueue a span whose
// epoch has changed since this snapshot, except the half a split in that same
// Plan explicitly moves.
func (b *Balancer) Observe(ctx context.Context) error {
	rows, err := b.DB.Placements(ctx)
	if err != nil {
		return err
	}
	seen := make(map[string]int64, len(rows))
	for _, row := range rows {
		seen[rangeKey(row.Tenant, row.Start)] = row.Epoch
	}
	b.seen = seen
	return nil
}

// Plan inserts at most one job. copied is how many records this pass already
// copied; once that reaches CopyCap, Plan inserts nothing.
func (b *Balancer) Plan(ctx context.Context, copied int64) (Action, error) {
	if b.CopyCap > 0 && copied >= b.CopyCap {
		return Action{Op: "none"}, nil
	}
	if b.Redis == nil {
		return Action{}, fmt.Errorf("redis is required to read rates")
	}
	rows, err := b.DB.Placements(ctx)
	if err != nil {
		return Action{}, err
	}
	rates, err := b.rates(ctx)
	if err != nil {
		return Action{}, err
	}
	rated := make([]ratedRange, 0, len(rows))
	for _, row := range rows {
		rated = append(rated, ratedRange{Placement: row, Rate: rates[rangeKey(row.Tenant, row.Start)]})
	}
	choice := choose(b.Owners, rated, b.seen)
	if choice.Op == "none" {
		return Action{Op: "none"}, nil
	}
	if choice.Op == "split" {
		left, right, err := b.DB.Split(ctx, choice.Tenant, choice.Start)
		if err != nil {
			return Action{}, err
		}
		b.Splits++
		hot := left
		hotRate := rates[rangeKey(left.Tenant, left.Start)]
		if rates[rangeKey(right.Tenant, right.Start)] > hotRate {
			hot = right
		}
		b.remember(left)
		b.remember(right)
		id, err := b.DB.InsertMove(ctx, hot.Tenant, hot.Start, hot.End, hot.Owner, choice.Destination)
		if err != nil {
			return Action{}, err
		}
		return Action{
			Op: "split", Tenant: hot.Tenant, Start: hot.Start, End: hot.End,
			Source: hot.Owner, Destination: choice.Destination, MoveID: id,
		}, nil
	}
	id, err := b.DB.InsertMove(ctx, choice.Tenant, choice.Start, choice.End, choice.Source, choice.Destination)
	if err != nil {
		return Action{}, err
	}
	if b.seen == nil {
		b.seen = map[string]int64{}
	}
	b.seen[rangeKey(choice.Tenant, choice.Start)] = choice.Epoch
	return Action{
		Op: "move", Tenant: choice.Tenant, Start: choice.Start, End: choice.End,
		Source: choice.Source, Destination: choice.Destination, MoveID: id,
	}, nil
}

func (b *Balancer) remember(rg store.Range) {
	if b.seen == nil {
		b.seen = map[string]int64{}
	}
	b.seen[rangeKey(rg.Tenant, rg.Start)] = rg.Epoch
}

func (b *Balancer) rates(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	iter := b.Redis.Scan(ctx, 0, "store:range:*", 200).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		rest := strings.TrimPrefix(key, "store:range:")
		cut := strings.LastIndex(rest, ":")
		if cut <= 0 {
			continue
		}
		start, err := strconv.ParseInt(rest[cut+1:], 10, 64)
		if err != nil {
			continue
		}
		n, err := b.Redis.Get(ctx, key).Int64()
		if err != nil {
			return nil, err
		}
		out[rangeKey(rest[:cut], start)] += n
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
