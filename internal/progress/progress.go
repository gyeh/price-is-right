package progress

import (
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// Tracker tracks progress for a single file.
type Tracker interface {
	SetStage(stage string)
	SetProgress(current, total int64)
	SetCounter(name string, value int64)
	LogWarning(msg string)
	Done()
}

// Manager creates trackers for individual files.
type Manager interface {
	NewTracker(index, total int, filename string) Tracker
	Wait()
	SetOverallStats(filesComplete, filesMatched int, totalRates int64)
	StartDiskMonitor(tmpDir string)
	StopDiskMonitor()
}

// MPBManager implements Manager using the mpb multi-progress-bar library.
type MPBManager struct {
	container  *mpb.Progress
	mu         sync.Mutex
	overallBar *mpb.Bar
	diskStop   chan struct{}
}

// NewMPBManager creates a new mpb-based progress manager.
func NewMPBManager() *MPBManager {
	p := mpb.New(mpb.WithWidth(60))
	return &MPBManager{container: p}
}

// NewTracker creates a new progress tracker for a file.
func (m *MPBManager) NewTracker(index, total int, filename string) Tracker {
	stageVal := &atomic.Value{}
	stageVal.Store("")
	detailVal := &atomic.Value{}
	detailVal.Store("")
	bar := m.container.AddBar(100,
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("[%d/%d] %s ", index+1, total, filename), decor.WCSyncSpaceR),
		),
		mpb.AppendDecorators(
			decor.Any(func(s decor.Statistics) string {
				stage := stageVal.Load().(string)
				detail := detailVal.Load().(string)
				if detail != "" {
					return stage + "  " + detail
				}
				return stage
			}),
		),
	)

	return &mpbTracker{
		bar:       bar,
		index:     index,
		total:     total,
		name:      filename,
		stagePtr:  stageVal,
		detailPtr: detailVal,
		mgr:       m,
	}
}

// Wait waits for all progress bars to finish.
func (m *MPBManager) Wait() {
	m.container.Wait()
}

// SetOverallStats updates overall progress (currently logged via stage names).
func (m *MPBManager) SetOverallStats(filesComplete, filesMatched int, totalRates int64) {
	// Could add an overall bar in the future
}

// StartDiskMonitor adds a status line showing real-time disk usage for tmpDir.
func (m *MPBManager) StartDiskMonitor(tmpDir string) {
	diskVal := &atomic.Value{}
	diskVal.Store("")

	m.mu.Lock()
	bar := m.container.AddBar(0,
		mpb.PrependDecorators(
			decor.Any(func(s decor.Statistics) string {
				return diskVal.Load().(string)
			}),
		),
	)
	m.mu.Unlock()

	m.diskStop = make(chan struct{})
	startTime := time.Now()
	// Snapshot initial usage to track delta from our process
	var baselineUsed uint64
	var stat0 syscall.Statfs_t
	if syscall.Statfs(tmpDir, &stat0) == nil {
		baselineUsed = (stat0.Blocks - stat0.Bavail) * uint64(stat0.Bsize)
	}
	var peakDelta uint64
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			elapsed := time.Since(startTime).Truncate(time.Second)
			var stat syscall.Statfs_t
			if err := syscall.Statfs(tmpDir, &stat); err == nil {
				avail := stat.Bavail * uint64(stat.Bsize)
				used := (stat.Blocks - stat.Bavail) * uint64(stat.Bsize)
				delta := uint64(0)
				if used > baselineUsed {
					delta = used - baselineUsed
				}
				if delta > peakDelta {
					peakDelta = delta
				}
				diskVal.Store(fmt.Sprintf("Elapsed: %s  |  Disk: %s used (peak %s), %s free",
					elapsed, humanBytesUint(delta), humanBytesUint(peakDelta), humanBytesUint(avail)))
			} else {
				diskVal.Store(fmt.Sprintf("Elapsed: %s", elapsed))
			}
			select {
			case <-ticker.C:
			case <-m.diskStop:
				bar.Abort(false)
				return
			}
		}
	}()
}

// StopDiskMonitor stops the disk usage monitor.
func (m *MPBManager) StopDiskMonitor() {
	if m.diskStop != nil {
		close(m.diskStop)
	}
}

type mpbTracker struct {
	bar         *mpb.Bar
	index       int
	total       int
	name        string
	stagePtr    *atomic.Value
	detailPtr   *atomic.Value // formatted download progress or counter detail
	mgr         *MPBManager
	// download speed tracking
	dlStart     time.Time // when first progress byte was seen
	dlPrevBytes int64     // bytes at last speed sample
	dlPrevTime  time.Time // time of last speed sample
	dlSpeed     float64   // smoothed MB/s
}

func (t *mpbTracker) SetStage(stage string) {
	t.stagePtr.Store(stage)
	t.detailPtr.Store("")
	t.bar.SetCurrent(0) // reset progress for new stage
	// Reset download speed tracking for new stage
	t.dlStart = time.Time{}
	t.dlPrevBytes = 0
	t.dlPrevTime = time.Time{}
	t.dlSpeed = 0
}

func (t *mpbTracker) SetProgress(current, total int64) {
	now := time.Now()

	// Initialize on first call
	if t.dlStart.IsZero() {
		t.dlStart = now
		t.dlPrevTime = now
		t.dlPrevBytes = current
	}

	// Compute speed from recent window (sample every 500ms to smooth jitter)
	speedStr := ""
	if elapsed := now.Sub(t.dlPrevTime).Seconds(); elapsed >= 0.5 {
		instantMBps := float64(current-t.dlPrevBytes) / elapsed / (1024 * 1024)
		// Exponential moving average (alpha=0.3) for smooth display
		if t.dlSpeed == 0 {
			t.dlSpeed = instantMBps
		} else {
			t.dlSpeed = 0.3*instantMBps + 0.7*t.dlSpeed
		}
		t.dlPrevBytes = current
		t.dlPrevTime = now
	}
	if t.dlSpeed > 0 {
		speedStr = fmt.Sprintf("  %.1f MB/s", t.dlSpeed)
	}

	if total > 0 {
		pct := int64(float64(current) / float64(total) * 100)
		t.bar.SetTotal(100, false)
		t.bar.SetCurrent(pct)
		t.detailPtr.Store(fmt.Sprintf("%s / %s%s", humanBytes(current), humanBytes(total), speedStr))
	} else if current > 0 {
		// Unknown total (Content-Length missing)
		t.detailPtr.Store(fmt.Sprintf("%s%s", humanBytes(current), speedStr))
	}
}

func (t *mpbTracker) SetCounter(name string, value int64) {
	t.detailPtr.Store(fmt.Sprintf("%s: %s", name, humanCount(value)))
}

func (t *mpbTracker) LogWarning(msg string) {
	// Write a persistent log line above the progress bars.
	// mpb.AddBar with a completed bar acts as a static log line.
	t.mgr.mu.Lock()
	defer t.mgr.mu.Unlock()
	logBar := t.mgr.container.AddBar(0,
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("  [%s] %s", t.name, msg)),
		),
	)
	logBar.Abort(false)
}

func (t *mpbTracker) Done() {
	t.bar.SetTotal(100, false)
	t.bar.SetCurrent(100)
	t.bar.Abort(false) // complete without removing
}

// NoopManager is a no-op progress manager for non-interactive use.
type NoopManager struct {
	FilesComplete int32
	FilesMatched  int32
	TotalRates    int64
}

func (m *NoopManager) NewTracker(index, total int, filename string) Tracker {
	return &noopTracker{mgr: m, name: filename}
}

func (m *NoopManager) Wait() {}
func (m *NoopManager) StartDiskMonitor(tmpDir string) {}
func (m *NoopManager) StopDiskMonitor()               {}

func (m *NoopManager) SetOverallStats(filesComplete, filesMatched int, totalRates int64) {
	atomic.StoreInt32(&m.FilesComplete, int32(filesComplete))
	atomic.StoreInt32(&m.FilesMatched, int32(filesMatched))
	atomic.StoreInt64(&m.TotalRates, totalRates)
}

type noopTracker struct {
	mgr  *NoopManager
	name string
}

func (t *noopTracker) SetStage(stage string) {
	fmt.Printf("  [%s] %s\n", t.name, stage)
}

func (t *noopTracker) SetProgress(current, total int64) {}
func (t *noopTracker) SetCounter(name string, value int64) {}
func (t *noopTracker) LogWarning(msg string) {
	fmt.Printf("  [%s] WARN: %s\n", t.name, msg)
}
func (t *noopTracker) Done() {}

// humanBytes formats a byte count as a human-readable string (e.g. "1.5 GB").
func humanBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func humanBytesUint(b uint64) string {
	const (
		kb uint64 = 1024
		mb        = 1024 * kb
		gb        = 1024 * mb
		tb        = 1024 * gb
	)
	switch {
	case b >= tb:
		return fmt.Sprintf("%.1f TB", float64(b)/float64(tb))
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// humanCount formats a number with comma separators (e.g. "1,234,567").
func humanCount(n int64) string {
	if n < 0 {
		return "-" + humanCount(-n)
	}
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return humanCount(n/1000) + fmt.Sprintf(",%03d", n%1000)
}
