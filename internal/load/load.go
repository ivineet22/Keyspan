// Package load sends a skewed workload and records how the requests landed.
package load

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"store/internal/hash"
)

// Report is one run of the generator.
type Report struct {
	ByShard map[string]float64
	Hottest string
	Share   float64
	Max     float64
	Mean    float64
	P50     time.Duration
	P99     time.Duration
	Errors  int
	Total   int
}

// Run sends requests against baseURL. About 80% use tenant "hot" and keys
// whose hash falls in a narrow band of the first shard's slice. The rest
// use tenant "other" and spread across the line.
func Run(ctx context.Context, baseURL string, requests int) (Report, error) {
	if requests < 1 {
		return Report{}, fmt.Errorf("requests must be positive")
	}
	hotKeys := keysInBand("hot", 16)
	latencies := make([]time.Duration, 0, requests)
	var mu sync.Mutex
	var errors int
	jobs := make(chan int)
	var wg sync.WaitGroup
	workers := 4
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				tenant := "other"
				key := fmt.Sprintf("spread-%d", i)
				if i%10 < 8 {
					tenant = "hot"
					key = hotKeys[i%len(hotKeys)]
				}
				url := fmt.Sprintf("%s/v1/tenants/%s/keys/%s", baseURL, tenant, key)
				start := time.Now()
				req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader([]byte("x")))
				if err != nil {
					mu.Lock()
					errors++
					mu.Unlock()
					continue
				}
				res, err := http.DefaultClient.Do(req)
				elapsed := time.Since(start)
				if err != nil {
					mu.Lock()
					errors++
					latencies = append(latencies, elapsed)
					mu.Unlock()
					continue
				}
				res.Body.Close()
				mu.Lock()
				latencies = append(latencies, elapsed)
				if res.StatusCode != http.StatusNoContent {
					errors++
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	counts, err := shardCounts(ctx, baseURL+"/metrics")
	if err != nil {
		return Report{}, err
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	report := Report{
		ByShard: counts,
		P50:     percentile(latencies, 50),
		P99:     percentile(latencies, 99),
		Errors:  errors,
		Total:   requests,
	}
	var sum float64
	for shard, n := range counts {
		sum += n
		if n > report.Max {
			report.Max = n
			report.Hottest = shard
		}
	}
	if len(counts) > 0 {
		report.Mean = sum / float64(len(counts))
	}
	if sum > 0 {
		report.Share = report.Max / sum
	}
	return report, nil
}

// WriteBaseline saves the measured fields. The numbers come from this run.
func WriteBaseline(path string, report Report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	var shards []string
	for name := range report.ByShard {
		shards = append(shards, name)
	}
	sort.Strings(shards)
	var b bytes.Buffer
	fmt.Fprintf(&b, "hottest_shard %s\n", report.Hottest)
	fmt.Fprintf(&b, "hottest_share %.4f\n", report.Share)
	fmt.Fprintf(&b, "max %.0f\n", report.Max)
	fmt.Fprintf(&b, "mean %.2f\n", report.Mean)
	if report.Mean > 0 {
		fmt.Fprintf(&b, "max_over_mean %.2f\n", report.Max/report.Mean)
	}
	fmt.Fprintf(&b, "p50_ms %.3f\n", float64(report.P50.Microseconds())/1000)
	fmt.Fprintf(&b, "p99_ms %.3f\n", float64(report.P99.Microseconds())/1000)
	fmt.Fprintf(&b, "errors %d\n", report.Errors)
	fmt.Fprintf(&b, "metric store_requests_total\n")
	for _, name := range shards {
		fmt.Fprintf(&b, "shard %s %.0f\n", name, report.ByShard[name])
	}
	return os.WriteFile(path, b.Bytes(), 0o644)
}

// Outcome is a rebalance measurement: the same fields as a load report, plus
// how the balancer got there.
type Outcome struct {
	Report
	RecordsCopied int64
	Splits        int
	MovesResumed  int
}

// WriteRebalanced saves the after-number file. The load fields are the same
// ones WriteBaseline writes.
func WriteRebalanced(path string, out Outcome) error {
	if err := WriteBaseline(path, out.Report); err != nil {
		return err
	}
	extra := fmt.Sprintf("records_copied %d\nsplits %d\nmoves_resumed %d\n", out.RecordsCopied, out.Splits, out.MovesResumed)
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(body, []byte(extra)...), 0o644)
}

// Isolate subtracts counts that were already on the metrics page, so a later
// run is not added to the earlier ones. Latency and errors stay from this run.
func Isolate(report Report, before map[string]float64) Report {
	if before == nil {
		return report
	}
	for shard, n := range report.ByShard {
		report.ByShard[shard] = n - before[shard]
	}
	var sum float64
	report.Max = 0
	report.Hottest = ""
	report.Share = 0
	for shard, n := range report.ByShard {
		if n < 0 {
			n = 0
			report.ByShard[shard] = 0
		}
		sum += n
		if n > report.Max {
			report.Max = n
			report.Hottest = shard
		}
	}
	if len(report.ByShard) > 0 {
		report.Mean = sum / float64(len(report.ByShard))
	}
	if sum > 0 {
		report.Share = report.Max / sum
	}
	return report
}

// ShardCounts reads store_requests_total from the router's metrics page.
func ShardCounts(ctx context.Context, metricsURL string) (map[string]float64, error) {
	return shardCounts(ctx, metricsURL)
}

func keysInBand(tenant string, want int) []string {
	end := hash.Parts(3)[0][1] / 16
	var keys []string
	for i := 0; len(keys) < want; i++ {
		key := fmt.Sprintf("hot-%d", i)
		place := int64(hash.Key(tenant, key))
		if place < end {
			keys = append(keys, key)
		}
	}
	return keys
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := len(sorted) * p / 100
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func shardCounts(ctx context.Context, metricsURL string) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	counts := map[string]float64{}
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "store_requests_total{") {
			continue
		}
		shard := metricLabel(line, "shard")
		if shard == "" || shard == "none" {
			continue
		}
		fields := strings.Fields(line)
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			return nil, err
		}
		counts[shard] += value
	}
	if len(counts) == 0 {
		return nil, fmt.Errorf("metric store_requests_total is missing")
	}
	return counts, nil
}

func metricLabel(line, name string) string {
	key := name + `="`
	i := strings.Index(line, key)
	if i < 0 {
		return ""
	}
	rest := line[i+len(key):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return ""
	}
	return rest[:j]
}
