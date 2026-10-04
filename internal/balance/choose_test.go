package balance

import (
	"testing"

	"store/internal/store"
)

func place(tenant, owner string, start, end, epoch, rate int64) ratedRange {
	return ratedRange{
		Placement: store.Placement{Range: store.Range{
			Tenant: tenant, Owner: owner, Start: start, End: end, Epoch: epoch,
		}},
		Rate: rate,
	}
}

func TestChooseSplitsTheRangeThatHoldsTheTraffic(t *testing.T) {
	owners := []string{"shard-1", "shard-2", "shard-3"}
	ranges := []ratedRange{
		place("hot", "shard-1", 0, 100, 1, 320),
		place("other", "shard-1", 0, 100, 1, 20),
		place("hot", "shard-2", 100, 200, 1, 10),
		place("hot", "shard-3", 200, 300, 1, 10),
	}
	got := choose(owners, ranges, map[string]int64{
		"hot/0": 1, "other/0": 1, "hot/100": 1, "hot/200": 1,
	})
	if got.Op != "split" || got.Tenant != "hot" || got.Start != 0 || got.Destination != "shard-2" {
		t.Fatalf("%+v", got)
	}
}

func TestChooseMovesAWarmRangeWhenNoSliceHoldsTheTraffic(t *testing.T) {
	owners := []string{"shard-1", "shard-2", "shard-3"}
	// Shard-1 has 140 of 220. The average shard is about 73, and 1.5 times that
	// is 110, so the shard is hot. Each of its slices is 70, which is half the
	// shard and under 110, and the quiet shard can take 70 without crossing 110.
	ranges := []ratedRange{
		place("a", "shard-1", 0, 50, 1, 70),
		place("b", "shard-1", 0, 50, 1, 70),
		place("a", "shard-2", 50, 100, 1, 40),
		place("a", "shard-3", 100, 150, 1, 40),
	}
	got := choose(owners, ranges, nil)
	if got.Op != "move" || got.Tenant != "a" || got.Destination != "shard-2" {
		t.Fatalf("%+v", got)
	}
}

func TestChooseSkipsARangeWhoseEpochChanged(t *testing.T) {
	owners := []string{"shard-1", "shard-2", "shard-3"}
	ranges := []ratedRange{
		place("hot", "shard-1", 0, 100, 3, 320),
		place("hot", "shard-2", 100, 200, 1, 10),
	}
	got := choose(owners, ranges, map[string]int64{"hot/0": 2, "hot/100": 1})
	if got.Op != "none" {
		t.Fatalf("%+v", got)
	}
}

func TestChooseStopsWhenNoShardIsHot(t *testing.T) {
	owners := []string{"shard-1", "shard-2", "shard-3"}
	ranges := []ratedRange{
		place("hot", "shard-1", 0, 50, 1, 140),
		place("hot", "shard-2", 50, 100, 1, 140),
		place("hot", "shard-3", 100, 150, 1, 120),
	}
	got := choose(owners, ranges, nil)
	if got.Op != "none" {
		t.Fatalf("%+v", got)
	}
}

func TestPlanRespectsTheCopyCap(t *testing.T) {
	b := &Balancer{CopyCap: 10}
	got, err := b.Plan(nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got.Op != "none" {
		t.Fatalf("%+v", got)
	}
}
