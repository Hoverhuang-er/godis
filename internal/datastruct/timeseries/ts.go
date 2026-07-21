package timeseries

import (
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
)

// LastTimestamp returns the timestamp of the most recent sample
func (ts *TimeSeries) LastTimestamp() int64 {
	slog.Debug("TimeSeries.LastTimestamp")
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.lastTimestamp
}

// Sample represents a single time-series data point
type Sample struct {
	Timestamp int64
	Value     float64
}

// TimeSeries stores a collection of timestamped samples with optional retention and labels
type TimeSeries struct {
	mu            sync.RWMutex
	key           string
	retention     int64
	labels        map[string]string
	samples       []Sample
	lastValue     float64
	lastTimestamp int64
}

// NewTimeSeries creates a new TimeSeries with the given key, retention, and labels
func NewTimeSeries(key string, retention int64, labels map[string]string) *TimeSeries {
	slog.Debug("NewTimeSeries", "key", key, "retention", retention)
	return &TimeSeries{
		key:       key,
		retention: retention,
		labels:    labels,
		samples:   make([]Sample, 0),
	}
}

// Add appends a sample to the time series, enforcing retention and monotonic timestamps
func (ts *TimeSeries) Add(timestamp int64, value float64) error {
	slog.Info("TimeSeries.Add", "key", ts.key, "timestamp", timestamp, "value", value)
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if timestamp == 0 {
		timestamp = time.Now().UnixMilli()
	}

	if len(ts.samples) > 0 && timestamp <= ts.samples[len(ts.samples)-1].Timestamp {
		timestamp = ts.samples[len(ts.samples)-1].Timestamp + 1
	}

	ts.samples = append(ts.samples, Sample{Timestamp: timestamp, Value: value})
	ts.lastValue = value
	ts.lastTimestamp = timestamp

	if ts.retention > 0 {
		cutoff := timestamp - ts.retention
		idx := sort.Search(len(ts.samples), func(i int) bool {
			return ts.samples[i].Timestamp >= cutoff
		})
		if idx > 0 {
			ts.samples = ts.samples[idx:]
		}
	}

	return nil
}

// Get retrieves the value at or before the given timestamp
func (ts *TimeSeries) Get(timestamp int64) (float64, bool) {
	slog.Debug("TimeSeries.Get", "key", ts.key, "timestamp", timestamp)
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	idx := sort.Search(len(ts.samples), func(i int) bool {
		return ts.samples[i].Timestamp >= timestamp
	})
	if idx < len(ts.samples) && ts.samples[idx].Timestamp == timestamp {
		return ts.samples[idx].Value, true
	}

	if len(ts.samples) == 0 {
		return 0, false
	}
	if timestamp >= ts.samples[len(ts.samples)-1].Timestamp {
		return ts.samples[len(ts.samples)-1].Value, true
	}
	if idx == 0 {
		return 0, false
	}
	return ts.samples[idx-1].Value, true
}

// Range returns samples within the time window, optionally aggregated into buckets
func (ts *TimeSeries) Range(start, end int64, count int, aggregation string, bucketDuration int64) []Sample {
	slog.Debug("TimeSeries.Range", "key", ts.key, "start", start, "end", end, "count", count)
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	if end == 0 {
		end = math.MaxInt64
	}

	result := make([]Sample, 0)
	for _, s := range ts.samples {
		if s.Timestamp >= start && s.Timestamp <= end {
			result = append(result, s)
		}
	}

	if len(result) == 0 {
		return result
	}

	if aggregation != "" && bucketDuration > 0 {
		result = aggregateSamples(result, aggregation, bucketDuration)
	}

	if count > 0 && len(result) > count {
		result = result[:count]
	}

	return result
}

func aggregateSamples(samples []Sample, aggregation string, bucketDuration int64) []Sample {
	slog.Debug("aggregateSamples", "aggregation", aggregation, "bucketDuration", bucketDuration)
	if bucketDuration <= 0 || len(samples) == 0 {
		return samples
	}

	buckets := make(map[int64]*struct {
		sum   float64
		min   float64
		max   float64
		count int
		first float64
		last  float64
	})

	var firstBucket, lastBucket int64
	for _, s := range samples {
		bucketKey := (s.Timestamp / bucketDuration) * bucketDuration
		if _, exists := buckets[bucketKey]; !exists {
			buckets[bucketKey] = &struct {
				sum   float64
				min   float64
				max   float64
				count int
				first float64
				last  float64
			}{
				min:   s.Value,
				max:   s.Value,
				first: s.Value,
				last:  s.Value,
			}
			if firstBucket == 0 || bucketKey < firstBucket {
				firstBucket = bucketKey
			}
			if bucketKey > lastBucket {
				lastBucket = bucketKey
			}
		}
		b := buckets[bucketKey]
		b.sum += s.Value
		b.count++
		if s.Value < b.min {
			b.min = s.Value
		}
		if s.Value > b.max {
			b.max = s.Value
		}
		b.last = s.Value
	}

	result := make([]Sample, 0)
	for bk := firstBucket; bk <= lastBucket; bk += bucketDuration {
		b, exists := buckets[bk]
		if !exists {
			continue
		}
		var v float64
		switch aggregation {
		case "AVG":
			v = b.sum / float64(b.count)
		case "SUM":
			v = b.sum
		case "MIN":
			v = b.min
		case "MAX":
			v = b.max
		case "RANGE":
			v = b.max - b.min
		case "COUNT":
			v = float64(b.count)
		case "FIRST":
			v = b.first
		case "LAST":
			v = b.last
		case "STD.P":
			mean := b.sum / float64(b.count)
			v = 0
			for _, s := range samples {
				bk2 := (s.Timestamp / bucketDuration) * bucketDuration
				if bk2 == bk {
					diff := s.Value - mean
					v += diff * diff
				}
			}
			v = math.Sqrt(v / float64(b.count))
		case "STD.S":
			mean := b.sum / float64(b.count)
			v = 0
			for _, s := range samples {
				bk2 := (s.Timestamp / bucketDuration) * bucketDuration
				if bk2 == bk {
					diff := s.Value - mean
					v += diff * diff
				}
			}
			if b.count > 1 {
				v = math.Sqrt(v / float64(b.count-1))
			}
		case "VAR.P":
			mean := b.sum / float64(b.count)
			v = 0
			for _, s := range samples {
				bk2 := (s.Timestamp / bucketDuration) * bucketDuration
				if bk2 == bk {
					diff := s.Value - mean
					v += diff * diff
				}
			}
			v = v / float64(b.count)
		case "VAR.S":
			mean := b.sum / float64(b.count)
			v = 0
			for _, s := range samples {
				bk2 := (s.Timestamp / bucketDuration) * bucketDuration
				if bk2 == bk {
					diff := s.Value - mean
					v += diff * diff
				}
			}
			if b.count > 1 {
				v = v / float64(b.count-1)
			}
		case "TWA":
			v = 0
			for i := 0; i < len(samples)-1; i++ {
				bk1 := (samples[i].Timestamp / bucketDuration) * bucketDuration
				bk2 := (samples[i+1].Timestamp / bucketDuration) * bucketDuration
				if bk1 == bk || bk2 == bk {
					duration := float64(samples[i+1].Timestamp - samples[i].Timestamp)
					v += samples[i].Value * duration
				}
			}
			v = v / float64(bucketDuration)
		default:
			v = b.sum / float64(b.count)
		}
		result = append(result, Sample{Timestamp: bk, Value: v})
	}

	return result
}

// Info returns metadata about the time series as a map
func (ts *TimeSeries) Info() map[string]interface{} {
	slog.Debug("TimeSeries.Info", "key", ts.key)
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	info := make(map[string]interface{})
	info["totalSamples"] = len(ts.samples)
	if len(ts.samples) > 0 {
		info["firstTimestamp"] = ts.samples[0].Timestamp
		info["lastTimestamp"] = ts.samples[len(ts.samples)-1].Timestamp
	}
	info["retention"] = ts.retention
	info["labels"] = ts.labels
	info["lastValue"] = ts.lastValue
	return info
}

// Len returns the number of samples in the time series
func (ts *TimeSeries) Len() int {
	slog.Debug("TimeSeries.Len", "key", ts.key)
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return len(ts.samples)
}

// Labels returns a copy of the time series labels
func (ts *TimeSeries) Labels() map[string]string {
	slog.Debug("TimeSeries.Labels", "key", ts.key)
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	cp := make(map[string]string, len(ts.labels))
	for k, v := range ts.labels {
		cp[k] = v
	}
	return cp
}

// MultiEntry aggregates the latest data across multiple time series for index queries
type MultiEntry struct {
	Key           string
	LastTimestamp int64
	LastValue     float64
	Labels        map[string]string
}

var (
	globalTSMu   sync.RWMutex
	globalTS     = make(map[string]*TimeSeries)
)

// RegisterTimeSeries registers a time series in the global registry by key
func RegisterTimeSeries(key string, ts *TimeSeries) {
	slog.Debug("RegisterTimeSeries", "key", key)
	globalTSMu.Lock()
	defer globalTSMu.Unlock()
	globalTS[key] = ts
}

// GetTimeSeries retrieves a time series from the global registry by key
func GetTimeSeries(key string) *TimeSeries {
	slog.Debug("GetTimeSeries", "key", key)
	globalTSMu.RLock()
	defer globalTSMu.RUnlock()
	return globalTS[key]
}

// DeleteTimeSeries removes a time series from the global registry by key
func DeleteTimeSeries(key string) {
	slog.Debug("DeleteTimeSeries", "key", key)
	globalTSMu.Lock()
	defer globalTSMu.Unlock()
	delete(globalTS, key)
}

// QueryByLabels returns all time series whose labels match the given filter
func QueryByLabels(labelFilter map[string]string) []*TimeSeries {
	slog.Debug("QueryByLabels")
	globalTSMu.RLock()
	defer globalTSMu.RUnlock()

	result := make([]*TimeSeries, 0)
	for _, ts := range globalTS {
		matches := true
		for k, v := range labelFilter {
			if tv, ok := ts.labels[k]; !ok || tv != v {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, ts)
		}
	}
	return result
}
