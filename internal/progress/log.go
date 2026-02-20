package progress

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// LogManager implements Manager with throttled line-based output for
// non-TTY environments (e.g. Modal, Fargate, CI). Prints periodic
// status lines instead of interactive progress bars.
type LogManager struct {
	mu       sync.Mutex
	diskStop chan struct{}
}

// NewLogManager creates a new log-based progress manager.
func NewLogManager() *LogManager {
	return &LogManager{}
}

func (m *LogManager) NewTracker(index, total int, filename string) Tracker {
	return &logTracker{
		mgr:   m,
		index: index,
		total: total,
		name:  filename,
		start: time.Now(),
	}
}

func (m *LogManager) Wait() {}

func (m *LogManager) SetOverallStats(filesComplete, filesMatched int, totalRates int64) {}

func (m *LogManager) StartDiskMonitor(tmpDir string) {}

func (m *LogManager) StopDiskMonitor() {}

func (m *LogManager) log(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts := time.Now().Format("15:04:05")
	fmt.Fprintf(os.Stderr, "%s %s\n", ts, msg)
}

// logTracker implements Tracker with throttled log output.
type logTracker struct {
	mgr       *LogManager
	index     int
	total     int
	name      string
	start     time.Time
	stage     string
	lastLog   time.Time
	prevBytes int64
	prevTime  time.Time
}

const logInterval = 20 * time.Second

func (t *logTracker) log(msg string) {
	t.mgr.mu.Lock()
	defer t.mgr.mu.Unlock()
	ts := time.Now().Format("15:04:05")
	fmt.Fprintf(os.Stderr, "%s [%d/%d] %s  %s\n", ts, t.index+1, t.total, t.name, msg)
}

func (t *logTracker) SetStage(stage string) {
	t.stage = stage
	t.lastLog = time.Time{} // reset throttle so next progress update prints
	t.prevBytes = 0
	t.prevTime = time.Time{}
	t.log(stage)
}

func (t *logTracker) SetProgress(current, total int64) {
	now := time.Now()
	if now.Sub(t.lastLog) < logInterval {
		return
	}

	// Compute speed since last logged progress line
	speedStr := ""
	if !t.prevTime.IsZero() {
		elapsed := now.Sub(t.prevTime).Seconds()
		if elapsed > 0 {
			mbps := float64(current-t.prevBytes) / elapsed / (1024 * 1024)
			speedStr = fmt.Sprintf("  %.1f MB/s", mbps)
		}
	}
	t.prevBytes = current
	t.prevTime = now
	t.lastLog = now

	if total > 0 {
		pct := float64(current) / float64(total) * 100
		t.log(fmt.Sprintf("%s  %s / %s (%.0f%%)%s", t.stage, humanBytes(current), humanBytes(total), pct, speedStr))
	} else if current > 0 {
		t.log(fmt.Sprintf("%s  %s%s", t.stage, humanBytes(current), speedStr))
	}
}

func (t *logTracker) SetCounter(name string, value int64) {
	if time.Since(t.lastLog) < logInterval {
		return
	}
	t.lastLog = time.Now()
	t.log(fmt.Sprintf("%s  %s: %s", t.stage, name, humanCount(value)))
}

func (t *logTracker) LogWarning(msg string) {
	t.log("WARN: " + msg)
}

func (t *logTracker) Done() {
	elapsed := time.Since(t.start).Truncate(time.Second)
	t.log(fmt.Sprintf("Finished in %s", elapsed))
}
