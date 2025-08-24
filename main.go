package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// PoolProcessor is the interface that jobs must implement to be processed by the worker pool
type PoolProcessor interface {
	Process(ctx context.Context) error
	GetID() string
}

// ErrorEntry represents a job processing error with metadata
type ErrorEntry struct {
	JobID     string
	Error     error
	Timestamp time.Time
	WorkerID  int
	IsPanic   bool
}

// Stats provides pool statistics
type Stats struct {
	TotalJobs     int64
	CompletedJobs int64
	FailedJobs    int64
	PanicJobs     int64
	ActiveWorkers int32
	QueuedJobs    int
	Errors        []ErrorEntry
}

// Worker represents a single worker in the pool
type Worker struct {
	workerID int
	logger   *slog.Logger
	pool     *Pool
}

// Pool manages a collection of workers and distributes jobs to them
type Pool struct {
	workers      []*Worker
	jobQueue     chan PoolProcessor
	workerWg     sync.WaitGroup
	jobWg        sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	shutdownOnce sync.Once

	// Error tracking
	errorsMu  sync.RWMutex
	errors    []ErrorEntry
	maxErrors int

	// Statistics (atomic counters for performance)
	totalJobs     int64
	completedJobs int64
	failedJobs    int64
	panicJobs     int64
	activeWorkers int32

	// Configuration
	queueSize int
	logger    *slog.Logger
	timeout   time.Duration // Per-job timeout
}

// PoolOption defines configuration options for the pool
type PoolOption func(*Pool)

// WithTimeout sets a per-job timeout
func WithTimeout(timeout time.Duration) PoolOption {
	return func(p *Pool) {
		p.timeout = timeout
	}
}

// NewPool creates a new worker pool with the specified number of workers
func NewPool(numWorkers int, queueSize int, maxErrors int, logger *slog.Logger, opts ...PoolOption) *Pool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &Pool{
		ctx:       ctx,
		cancel:    cancel,
		jobQueue:  make(chan PoolProcessor, queueSize),
		queueSize: queueSize,
		maxErrors: maxErrors,
		errors:    make([]ErrorEntry, 0, maxErrors),
		logger:    logger,
		timeout:   0, // No timeout by default
	}

	// Apply options
	for _, opt := range opts {
		opt(pool)
	}

	// Initialize workers
	pool.workers = make([]*Worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		pool.workers[i] = &Worker{
			workerID: i,
			logger:   logger,
			pool:     pool,
		}
	}

	return pool
}

// Add submits a new job to the pool
func (p *Pool) Add(job PoolProcessor) error {
	if job == nil {
		return fmt.Errorf("job cannot be nil")
	}
	p.jobWg.Add(1)
	select {
	case p.jobQueue <- job:
		atomic.AddInt64(&p.totalJobs, 1)
		return nil
	case <-p.ctx.Done():
		p.jobWg.Done()
		return fmt.Errorf("pool is shutting down")
	}
}

// AddBatch adds multiple jobs to the pool at once
func (p *Pool) AddBatch(jobs []PoolProcessor) error {
	for _, job := range jobs {
		if err := p.Add(job); err != nil {
			return fmt.Errorf("failed to add job %s: %w", job.GetID(), err)
		}
	}
	return nil
}

// Start begins worker execution
func (p *Pool) Start() {
	for _, worker := range p.workers {
		p.workerWg.Add(1)
		go worker.start()
	}
}

// Stop gracefully shuts down the worker pool
func (p *Pool) Stop() {
	p.shutdownOnce.Do(func() {
		p.cancel()
		p.workerWg.Wait()
		close(p.jobQueue)
	})
}

// Wait waits for all jobs to complete and then stops the pool
func (p *Pool) Wait() []error {
	p.jobWg.Wait()
	p.Stop()
	return p.GetErrors()
}

// GetStats returns current pool statistics
func (p *Pool) GetStats() Stats {
	p.errorsMu.RLock()
	errors := make([]ErrorEntry, len(p.errors))
	copy(errors, p.errors)
	p.errorsMu.RUnlock()

	return Stats{
		TotalJobs:     atomic.LoadInt64(&p.totalJobs),
		CompletedJobs: atomic.LoadInt64(&p.completedJobs),
		FailedJobs:    atomic.LoadInt64(&p.failedJobs),
		PanicJobs:     atomic.LoadInt64(&p.panicJobs),
		ActiveWorkers: atomic.LoadInt32(&p.activeWorkers),
		QueuedJobs:    len(p.jobQueue),
		Errors:        errors,
	}
}

// GetErrors returns all recorded errors
func (p *Pool) GetErrors() []error {
	p.errorsMu.RLock()
	defer p.errorsMu.RUnlock()

	result := make([]error, len(p.errors))
	for i, entry := range p.errors {
		result[i] = fmt.Errorf("worker %d, job %s: %w", entry.WorkerID, entry.JobID, entry.Error)
	}
	return result
}

// recordError adds an error to the pool's error collection
func (p *Pool) recordError(jobID string, err error, workerID int, isPanic bool) {
	p.errorsMu.Lock()
	defer p.errorsMu.Unlock()

	entry := ErrorEntry{
		JobID:     jobID,
		Error:     err,
		Timestamp: time.Now(),
		WorkerID:  workerID,
		IsPanic:   isPanic,
	}

	// Maintain a circular buffer for errors
	if len(p.errors) >= p.maxErrors {
		p.errors = append(p.errors[1:], entry)
	} else {
		p.errors = append(p.errors, entry)
	}
}

// Worker methods

func (w *Worker) Done() {
	w.pool.workerWg.Done()
}

func (w *Worker) start() {
	defer w.Done()

	for {
		select {
		case job, ok := <-w.pool.jobQueue:
			if !ok {
				w.logger.Info("Worker shutting down", slog.Int("workerID", w.workerID))
				return
			}
			w.processJob(job)

		case <-w.pool.ctx.Done():
			w.logger.Info("Worker stopping due to context cancellation", slog.Int("workerID", w.workerID))
			return
		}
	}
}

func (w *Worker) processJob(job PoolProcessor) {
	atomic.AddInt32(&w.pool.activeWorkers, 1)
	defer func() {
		atomic.AddInt32(&w.pool.activeWorkers, -1)
		w.pool.jobWg.Done()
	}()

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			atomic.AddInt64(&w.pool.panicJobs, 1)

			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			stackTrace := string(buf[:n])

			panicErr := fmt.Errorf("panic: %v\nstack: %s", r, stackTrace)
			w.pool.recordError(job.GetID(), panicErr, w.workerID, true)

			w.logger.Error("Worker panic recovered",
				slog.Int("workerID", w.workerID),
				slog.String("jobID", job.GetID()),
				slog.Any("panic", r),
				slog.String("stackTrace", stackTrace))
		}
	}()

	w.logger.Debug("Processing job",
		slog.Int("workerID", w.workerID),
		slog.String("jobID", job.GetID()))

	startTime := time.Now()

	// Create job context with optional timeout
	jobCtx := w.pool.ctx
	var cancel context.CancelFunc
	if w.pool.timeout > 0 {
		jobCtx, cancel = context.WithTimeout(w.pool.ctx, w.pool.timeout)
		defer cancel()
	}

	// Process the job
	err := job.Process(jobCtx)
	duration := time.Since(startTime)

	if err != nil {
		atomic.AddInt64(&w.pool.failedJobs, 1)
		w.pool.recordError(job.GetID(), err, w.workerID, false)

		w.logger.Error("Job processing failed",
			slog.Int("workerID", w.workerID),
			slog.String("jobID", job.GetID()),
			slog.String("error", err.Error()),
			slog.Duration("duration", duration))
	} else {
		atomic.AddInt64(&w.pool.completedJobs, 1)
		w.logger.Debug("Job completed successfully",
			slog.Int("workerID", w.workerID),
			slog.String("jobID", job.GetID()),
			slog.Duration("duration", duration))
	}
}
