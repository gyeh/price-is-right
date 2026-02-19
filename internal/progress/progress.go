package progress

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// Tracker tracks progress for a single file.
type Tracker interface {
	SetStage(stage string)
	SetProgress(current, total int64)
	SetCounter(name string, value int64)
	Done()
}

// Manager creates trackers for individual files.
type Manager interface {
	NewTracker(index, total int, filename string) Tracker
	Wait()
	SetOverallStats(filesComplete, filesMatched int, totalRates int64)
}

// MPBManager implements Manager using the mpb multi-progress-bar library.
type MPBManager struct {
	container *mpb.Progress
	mu        sync.Mutex
	overallBar *mpb.Bar
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
	bar := m.container.AddBar(100,
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("[%d/%d] %s ", index+1, total, filename), decor.WCSyncSpaceR),
		),
		mpb.AppendDecorators(
			decor.Any(func(s decor.Statistics) string {
				return stageVal.Load().(string)
			}),
		),
	)

	return &mpbTracker{
		bar:      bar,
		index:    index,
		total:    total,
		name:     filename,
		stagePtr: stageVal,
		mgr:      m,
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

type mpbTracker struct {
	bar      *mpb.Bar
	index    int
	total    int
	name     string
	stagePtr *atomic.Value
	mgr      *MPBManager
}

func (t *mpbTracker) SetStage(stage string) {
	t.stagePtr.Store(stage)
	t.bar.SetCurrent(0) // reset progress for new stage
}

func (t *mpbTracker) SetProgress(current, total int64) {
	if total > 0 {
		pct := int64(float64(current) / float64(total) * 100)
		t.bar.SetTotal(100, false)
		t.bar.SetCurrent(pct)
	}
}

func (t *mpbTracker) SetCounter(name string, value int64) {
	// Counters are informational; we update the bar progress proportionally
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
func (t *noopTracker) Done() {}
