// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package udpserver

import (
	"context"
	"sync"
	"sync/atomic"

	"masterdnsvpn-go/internal/logger"
)

type deferredSessionLane struct {
	sessionID uint8
	streamID  uint16
}

type deferredSessionTask struct {
	lane deferredSessionLane
	run  func()
}

type deferredSessionWorker struct {
	jobs    chan deferredSessionTask
	pending atomic.Int32
}

type deferredSessionProcessor struct {
	log        *logger.Logger
	workers    []deferredSessionWorker
	mu         sync.Mutex
	laneWorker map[deferredSessionLane]int
	nextWorker int
}

func newDeferredSessionProcessor(workerCount int, queueLimit int, log *logger.Logger) *deferredSessionProcessor {
	if workerCount <= 0 {
		return nil
	}
	if workerCount > 64 {
		workerCount = 64
	}
	if queueLimit < 1 {
		queueLimit = 256
	}
	if queueLimit > 8192 {
		queueLimit = 8192
	}

	workers := make([]deferredSessionWorker, workerCount)
	for i := range workers {
		workers[i].jobs = make(chan deferredSessionTask, queueLimit)
	}

	return &deferredSessionProcessor{
		log:        log,
		workers:    workers,
		laneWorker: make(map[deferredSessionLane]int, 128),
	}
}

func (p *deferredSessionProcessor) Start(ctx context.Context) {
	if p == nil {
		return
	}
	for idx := range p.workers {
		go p.runWorker(ctx, idx)
	}
}

func (p *deferredSessionProcessor) Enqueue(lane deferredSessionLane, run func()) bool {
	if p == nil || run == nil || len(p.workers) == 0 {
		return false
	}

	task := deferredSessionTask{
		lane: lane,
		run:  run,
	}

	p.mu.Lock()
	if workerIdx, ok := p.laneWorker[lane]; ok {
		ok = p.enqueueToExistingWorkerLocked(workerIdx, task)
		p.mu.Unlock()
		return ok
	}

	start := p.nextWorker
	workerIdx := p.selectLeastLoadedWorkerLocked(start)
	if workerIdx < 0 {
		p.mu.Unlock()
		return false
	}
	p.nextWorker = (workerIdx + 1) % len(p.workers)
	p.laneWorker[lane] = workerIdx
	ok := p.tryEnqueueLocked(workerIdx, task)
	if !ok {
		delete(p.laneWorker, lane)
	}
	p.mu.Unlock()
	return ok
}

func (p *deferredSessionProcessor) RemoveSession(sessionID uint8) {
	if p == nil {
		return
	}
	p.mu.Lock()
	for lane := range p.laneWorker {
		if lane.sessionID == sessionID {
			delete(p.laneWorker, lane)
		}
	}
	p.mu.Unlock()
}

func (p *deferredSessionProcessor) runWorker(ctx context.Context, workerIdx int) {
	worker := &p.workers[workerIdx]
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-worker.jobs:
			if !ok {
				return
			}
			func() {
				defer func() {
					worker.pending.Add(-1)
					if recovered := recover(); recovered != nil && p.log != nil {
						p.log.Errorf(
							"💥 <red>Deferred Session Worker Panic, Worker: <cyan>%d</cyan>, Session: <cyan>%d</cyan>, Stream: <cyan>%d</cyan>, Error: <yellow>%v</yellow></red>",
							workerIdx+1,
							task.lane.sessionID,
							task.lane.streamID,
							recovered,
						)
					}
				}()
				task.run()
			}()
		}
	}
}

func (p *deferredSessionProcessor) enqueueToExistingWorkerLocked(workerIdx int, task deferredSessionTask) bool {
	worker := &p.workers[workerIdx]
	worker.pending.Add(1)
	select {
	case worker.jobs <- task:
		return true
	default:
		select {
		case worker.jobs <- task:
			return true
		default:
			worker.pending.Add(-1)
			return false
		}
	}
}

func (p *deferredSessionProcessor) tryEnqueueLocked(workerIdx int, task deferredSessionTask) bool {
	worker := &p.workers[workerIdx]
	worker.pending.Add(1)
	select {
	case worker.jobs <- task:
		return true
	default:
		worker.pending.Add(-1)
		return false
	}
}

func (p *deferredSessionProcessor) selectLeastLoadedWorkerLocked(start int) int {
	if len(p.workers) == 0 {
		return -1
	}

	bestIdx := -1
	bestPending := int32(0)
	for offset := range p.workers {
		idx := (start + offset) % len(p.workers)
		pending := p.workers[idx].pending.Load()
		if bestIdx < 0 || pending < bestPending {
			bestIdx = idx
			bestPending = pending
			if pending == 0 {
				break
			}
		}
	}
	return bestIdx
}
