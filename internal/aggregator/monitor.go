package aggregator

import (
	"sync"
	"time"
)

type event struct {
	ts      time.Time
	isError bool
}

// ErrorRateMonitor tracks per-service error rate in a sliding window.
type ErrorRateMonitor struct {
	mu           sync.Mutex
	windows      map[string][]event
	windowSize   time.Duration
	thresholdPct float64
	lastAlert    map[string]time.Time
	alertDebounce time.Duration
	OnAlert      func(service string, rate float64)
}

func NewErrorRateMonitor(windowSec int, thresholdPct float64) *ErrorRateMonitor {
	return &ErrorRateMonitor{
		windows:       make(map[string][]event),
		windowSize:    time.Duration(windowSec) * time.Second,
		thresholdPct:  thresholdPct,
		lastAlert:     make(map[string]time.Time),
		alertDebounce: time.Minute,
	}
}

func (m *ErrorRateMonitor) Record(service string, isError bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-m.windowSize)

	w := m.windows[service]
	// evict old events
	i := 0
	for i < len(w) && w[i].ts.Before(cutoff) {
		i++
	}
	w = append(w[i:], event{ts: now, isError: isError})
	m.windows[service] = w

	if len(w) < 10 {
		return // not enough data
	}

	var errors int
	for _, e := range w {
		if e.isError {
			errors++
		}
	}
	rate := float64(errors) / float64(len(w)) * 100

	if rate >= m.thresholdPct {
		if time.Since(m.lastAlert[service]) >= m.alertDebounce {
			m.lastAlert[service] = now
			if m.OnAlert != nil {
				go m.OnAlert(service, rate)
			}
		}
	}
}
