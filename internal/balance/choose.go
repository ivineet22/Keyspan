package balance

import (
	"sort"

	"store/internal/store"
)

type ratedRange struct {
	store.Placement
	Rate int64
}

type choice struct {
	Op                  string
	Tenant              string
	Start, End          int64
	Source, Destination string
	Epoch               int64
}

// choose picks one action from the current rates.
// A shard is hot when its requests are above 1.5 times the average shard.
// If one of its slices alone is that hot, the slice is split. Moving it whole
// would only hand the same crowd to another shard.
// If the shard is hot but no single slice is, and it has two or more slices,
// the busiest slice is moved to the quietest shard.
// A slice whose epoch changed since seen, or that already has a job, is left alone.
func choose(owners []string, ranges []ratedRange, seen map[string]int64) choice {
	if len(owners) == 0 {
		return choice{Op: "none"}
	}
	byShard := map[string]int64{}
	for _, name := range owners {
		byShard[name] = 0
	}
	for _, rg := range ranges {
		byShard[rg.Owner] += rg.Rate
	}
	var total int64
	for _, name := range owners {
		total += byShard[name]
	}
	n := len(owners)
	hottest := owners[0]
	for _, name := range owners[1:] {
		if byShard[name] > byShard[hottest] || (byShard[name] == byShard[hottest] && name < hottest) {
			hottest = name
		}
	}
	if !above(byShard[hottest], total, n) {
		return choice{Op: "none"}
	}
	var onHot []ratedRange
	for _, rg := range ranges {
		if rg.Owner != hottest || rg.Busy || !settled(rg, seen) {
			continue
		}
		onHot = append(onHot, rg)
	}
	if len(onHot) == 0 {
		return choice{Op: "none"}
	}
	sort.Slice(onHot, func(i, j int) bool {
		if onHot[i].Rate != onHot[j].Rate {
			return onHot[i].Rate > onHot[j].Rate
		}
		if onHot[i].Tenant != onHot[j].Tenant {
			return onHot[i].Tenant < onHot[j].Tenant
		}
		return onHot[i].Start < onHot[j].Start
	})
	hot := onHot[0]
	cold := coldest(owners, byShard, hottest)
	if cold == "" {
		return choice{Op: "none"}
	}
	// One slice holds this shard's traffic when it is more than half of that
	// shard, or when it alone is above the line. Cutting it is what spreads
	// the keys. Moving it whole would hand the same crowd to the quiet shard.
	holds := hot.Rate*2 > byShard[hottest] || above(hot.Rate, total, n)
	if holds && hot.End-hot.Start >= 2 {
		return choice{
			Op: "split", Tenant: hot.Tenant, Start: hot.Start, End: hot.End,
			Source: hot.Owner, Destination: cold, Epoch: hot.Epoch,
		}
	}
	// A whole-slice move is useful when the quiet shard can take it without
	// becoming the new hot shard.
	if len(onHot) >= 2 && !above(byShard[cold]+hot.Rate, total, n) {
		return choice{
			Op: "move", Tenant: hot.Tenant, Start: hot.Start, End: hot.End,
			Source: hot.Owner, Destination: cold, Epoch: hot.Epoch,
		}
	}
	if hot.End-hot.Start >= 2 {
		return choice{
			Op: "split", Tenant: hot.Tenant, Start: hot.Start, End: hot.End,
			Source: hot.Owner, Destination: cold, Epoch: hot.Epoch,
		}
	}
	return choice{Op: "none"}
}

func settled(rg ratedRange, seen map[string]int64) bool {
	if seen == nil {
		return true
	}
	epoch, ok := seen[rangeKey(rg.Tenant, rg.Start)]
	if !ok {
		return true
	}
	return epoch == rg.Epoch
}

func coldest(owners []string, byShard map[string]int64, skip string) string {
	best := ""
	for _, name := range owners {
		if name == skip {
			continue
		}
		if best == "" || byShard[name] < byShard[best] || (byShard[name] == byShard[best] && name < best) {
			best = name
		}
	}
	return best
}

// above reports whether rate is strictly greater than 1.5 times total/n.
func above(rate, total int64, n int) bool {
	if n < 1 || rate <= 0 {
		return false
	}
	return rate*int64(n)*2 > total*3
}
