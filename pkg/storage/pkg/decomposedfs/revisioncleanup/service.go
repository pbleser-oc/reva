// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisioncleanup

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"

	"github.com/rs/zerolog"

	"github.com/opencloud-eu/reva/v2/pkg/appctx"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/node"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/options"
)

// QueueFullPolicy defines what happens when a cleanup is scheduled while the queue is full.
type QueueFullPolicy string

const (
	// QueueFullBlock makes the caller wait until there is room in the queue, so that no
	// cleanup is ever skipped, at the cost of slowing down writes under heavy load.
	QueueFullBlock QueueFullPolicy = "block"
	// QueueFullDrop skips the cleanup and logs it, so that writes never wait for the
	// cleanup. The node is cleaned up again when it gets its next revision.
	QueueFullDrop QueueFullPolicy = "drop"
)

// queueSizePerWorker is the number of queue slots per worker when the queue size is
// determined automatically.
const queueSizePerWorker = 64

// AsyncOptions configures how a Service runs the cleanup. They are passed as
// parameters rather than read from the driver options, so that other callers, like a
// command line tool, can use their own settings.
type AsyncOptions struct {
	// Enabled runs the cleanup in background workers. Otherwise HandleRevisionCreated runs it
	// right away, in the caller's goroutine.
	Enabled bool
	// Workers is the number of background workers, 0 determines it automatically:
	// half the available CPUs, but at least 2.
	Workers int
	// QueueSize is the number of nodes that can wait for a cleanup, 0 determines it
	// automatically from the number of workers.
	QueueSize int
	// QueueFull is the policy when the queue is full, "" means QueueFullBlock.
	QueueFull QueueFullPolicy
}

// resolve validates the options and determines the automatic values.
func (o AsyncOptions) resolve() (workers int, queueSize int, policy QueueFullPolicy, err error) {
	switch {
	case o.Workers < 0:
		return 0, 0, "", fmt.Errorf("invalid number of revision cleanup workers %d", o.Workers)
	case o.Workers == 0:
		// GOMAXPROCS rather than NumCPU, as it respects container CPU limits (cgroups)
		workers = max(2, runtime.GOMAXPROCS(0)/2)
	default:
		workers = o.Workers
	}

	switch {
	case o.QueueSize < 0:
		return 0, 0, "", fmt.Errorf("invalid revision cleanup queue size %d", o.QueueSize)
	case o.QueueSize == 0:
		queueSize = workers * queueSizePerWorker
	default:
		queueSize = o.QueueSize
	}

	switch o.QueueFull {
	case "":
		policy = QueueFullBlock
	case QueueFullBlock, QueueFullDrop:
		policy = o.QueueFull
	default:
		return 0, 0, "", fmt.Errorf("invalid revision cleanup queue full policy %q, must be %q or %q", o.QueueFull, QueueFullBlock, QueueFullDrop)
	}
	return workers, queueSize, policy, nil
}

// Service is the revision cleanup of a storage driver. Callers report that a new revision
// was created (HandleRevisionCreated), and the Service decides whether, when and how the
// old revisions are cleaned up; callers don't need to know whether that happens right away
// or in background workers (see AsyncOptions). It binds a Cleaner to the driver's lookup
// and tree, so that callers only need to name the node.
// A nil *Service is valid and never cleans up anything.
//
// In asynchronous mode, the cleanup runs in background workers fed by a bounded queue, so
// that it doesn't slow down the upload, restore or change on disk that created the new
// revision. Queued requests only carry the space and node id, and the worker reads the
// node again: a *node.Node must not be shared between goroutines, as its lock-held flag is
// per goroutine and its cached attributes go stale.
//
// Requests are de-duplicated per node: a node that is already waiting in the queue is not
// queued again (the revisions to keep are merged into the waiting request). A node whose
// cleanup is already running is not queued either, as another worker would only wait for
// the node lock: when that cleanup is done, the node is queued once more, at the end of the
// queue, as new revisions may have been created in the meantime, with the revisions to keep
// of all the requests that came in until then. A node therefore never occupies more than
// one worker, and a single busy file can't keep the workers from the others.
type Service struct {
	cleaner *Cleaner
	lu      node.PathLookup
	rs      RevisionStore
	log     *zerolog.Logger

	// set for the Service of an on-demand Run: only decide, don't delete, and hand the Result
	// of every cleanup to report
	dryRun bool
	report func(Result)
	// the metrics this Service updates, see m
	metrics *serviceMetrics

	async   bool
	policy  QueueFullPolicy
	process func(ctx context.Context, spaceID, nodeID string, keep []string)
	queue   chan *request
	done    chan struct{}
	wg      sync.WaitGroup

	// the numbers this Service added to the Workers and QueueCapacity gauges
	workers   int
	queueSize int

	mu     sync.Mutex
	queued map[string]*request // the requests waiting in the queue, by node; QueueLength counts them
	// the nodes being cleaned up by a worker, with the request to queue once more when it
	// is done, nil if there is none; QueueLength counts those requests as well
	running map[string]*request
	closed  bool
	// pending counts the requests from being queued until their cleanup has finished (or they
	// were dropped or abandoned); idle is closed whenever pending is 0, see Wait
	pending int
	idle    chan struct{}
}

type request struct {
	ctx     context.Context
	spaceID string
	nodeID  string
	keep    []string
}

func (r *request) key() string {
	return r.spaceID + "/" + r.nodeID
}

// merge adds the revisions to keep of other to r, s.mu must be held.
func (r *request) merge(other *request) {
	for _, k := range other.keep {
		if !slices.Contains(r.keep, k) {
			r.keep = append(r.keep, k)
		}
	}
}

// NewServiceFromOptions creates the Service of a storage driver from its options: the cleanup
// strategy (RevisionsCleanupStrategy) and the RevisionsCleanupAsync* settings.
// It returns nil when no strategy is configured, and an error for invalid settings.
func NewServiceFromOptions(o *options.Options, lu node.PathLookup, rs RevisionStore, log *zerolog.Logger) (*Service, error) {
	cleaner, err := New(o.RevisionsCleanupStrategy)
	if err != nil {
		return nil, err
	}
	return NewService(cleaner, lu, rs, AsyncOptionsFromOptions(o), log)
}

// AsyncOptionsFromOptions returns the AsyncOptions configured in the driver options
// (the RevisionsCleanupAsync* settings).
func AsyncOptionsFromOptions(o *options.Options) AsyncOptions {
	return AsyncOptions{
		Enabled:   o.RevisionsCleanupAsync,
		Workers:   o.RevisionsCleanupAsyncWorkers,
		QueueSize: o.RevisionsCleanupAsyncQueueSize,
		QueueFull: QueueFullPolicy(o.RevisionsCleanupAsyncQueueFull),
	}
}

// NewService creates a Service that cleans up with the given Cleaner, and starts its
// workers if opts.Enabled. It returns nil if cleaner is nil, i.e. the cleanup is disabled.
// The options are validated in any case, so that invalid settings are reported even while
// the cleanup is disabled.
func NewService(cleaner *Cleaner, lu node.PathLookup, rs RevisionStore, opts AsyncOptions, log *zerolog.Logger) (*Service, error) {
	return newService(cleaner, lu, rs, opts, log, false, false, nil)
}

// newService creates a Service, see NewService; onDemand creates the Service of an on-demand
// Run, with dryRun it only decides without deleting, and report, if not nil, gets the Result
// of every cleanup.
func newService(cleaner *Cleaner, lu node.PathLookup, rs RevisionStore, opts AsyncOptions, log *zerolog.Logger, onDemand, dryRun bool, report func(Result)) (*Service, error) {
	workers, queueSize, policy, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	if cleaner == nil {
		return nil, nil
	}
	if log == nil {
		log = &zerolog.Logger{}
	}
	s := &Service{cleaner: cleaner, lu: lu, rs: rs, log: log, dryRun: dryRun, report: report}
	if onDemand {
		s.metrics = unregisteredMetrics()
	}
	s.m().enabled.Set(1)
	if opts.Enabled {
		s.startWorkers(workers, queueSize, policy, s.cleanupNode)
		log.Info().Str("config", cleaner.config).Int("workers", workers).Int("queuesize", queueSize).
			Str("queuefull", string(policy)).Msg("revision cleanup: running in the background")
	}
	return s, nil
}

// startWorkers switches the Service to asynchronous mode. process does the work for one
// node; it is a parameter so that tests can observe the queueing without a storage.
func (s *Service) startWorkers(workers, queueSize int, policy QueueFullPolicy, process func(ctx context.Context, spaceID, nodeID string, keep []string)) {
	s.async = true
	s.policy = policy
	s.process = process
	s.queue = make(chan *request, queueSize)
	s.done = make(chan struct{})
	s.queued = make(map[string]*request)
	s.running = make(map[string]*request)
	s.idle = make(chan struct{})
	close(s.idle)
	s.workers = workers
	s.queueSize = queueSize
	s.m().workers.Add(float64(workers))
	s.m().queueCapacity.Add(float64(queueSize))
	for range workers {
		s.wg.Add(1)
		go s.work()
	}
}

// HandleRevisionCreated handles the event that a new revision of n was created and has
// settled: it cleans up the old revisions of n according to the strategy, never deleting
// the revisions with a key in keep. In asynchronous mode it only queues n and returns,
// see Service and AsyncOptions.
// Errors are logged, never returned: a failed cleanup must not fail the operation that
// triggered it.
func (s *Service) HandleRevisionCreated(ctx context.Context, n *node.Node, keep ...string) {
	if s == nil || n == nil {
		return
	}
	if !s.async {
		s.clean(ctx, n, keep)
		return
	}
	// the cleanup outlives the request that triggered it, but keeps its values,
	// e.g. the logger and the trace context
	s.enqueue(&request{ctx: context.WithoutCancel(ctx), spaceID: n.SpaceID, nodeID: n.ID, keep: slices.Clone(keep)})
}

func (s *Service) enqueue(r *request) {
	log := appctx.GetLogger(r.ctx).With().Str("spaceid", r.spaceID).Str("nodeid", r.nodeID).Logger()
	key := r.key()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		log.Debug().Msg("revision cleanup: shutting down, not scheduling cleanup")
		return
	}
	if waiting, ok := s.queued[key]; ok {
		// already waiting in the queue, it will see the new revision as well
		waiting.merge(r)
		s.mu.Unlock()
		return
	}
	if again, ok := s.running[key]; ok {
		// being cleaned up right now: it is queued once more when that is done
		if again == nil {
			s.running[key] = r
			s.addPending()
			s.m().queueLength.Inc()
		} else {
			again.merge(r)
		}
		s.mu.Unlock()
		return
	}
	s.queued[key] = r
	s.addPending()
	s.m().queueLength.Inc()
	s.mu.Unlock()
	s.send(r)
}

// send puts r, which waits in s.queued, into the queue, applying the queue full policy.
// s.mu is not held while sending, so that a blocked sender does not block the workers.
func (s *Service) send(r *request) {
	log := appctx.GetLogger(r.ctx).With().Str("spaceid", r.spaceID).Str("nodeid", r.nodeID).Logger()
	select {
	case s.queue <- r:
		return
	default:
	}
	if s.policy == QueueFullDrop {
		s.m().queueFull.WithLabelValues(queueFullDropped).Inc()
		s.reportResult(Result{SpaceID: r.spaceID, NodeID: r.nodeID, DryRun: s.dryRun,
			Skipped: fmt.Sprintf("the queue was full (queue full policy %q)", QueueFullDrop)})
		s.forget(r)
		log.Warn().Msg("revision cleanup: queue is full, skipping the cleanup of this node until its next revision")
		return
	}
	s.m().queueFull.WithLabelValues(queueFullBlocked).Inc()
	log.Debug().Msg("revision cleanup: queue is full, waiting for room")
	select {
	case s.queue <- r:
	case <-s.done:
		s.forget(r)
	}
}

// forget removes a request that did not make it into the queue.
func (s *Service) forget(r *request) {
	s.mu.Lock()
	if s.queued[r.key()] == r {
		delete(s.queued, r.key())
		s.m().queueLength.Dec()
		s.donePending(1)
	}
	s.mu.Unlock()
}

func (s *Service) work() {
	defer s.wg.Done()
	for {
		// a select with several ready cases picks one at random, so check for the
		// shutdown first, otherwise a worker could keep taking requests from a
		// non-empty queue after Shutdown
		select {
		case <-s.done:
			return
		default:
		}
		select {
		case <-s.done:
			return
		case r := <-s.queue:
			select {
			case <-s.done:
				// shut down while waiting for this request: abandon it, Shutdown
				// counts it as abandoned
				return
			default:
			}
			// from here on, new requests for the node are collected to queue it once more
			// when this cleanup is done, instead of being merged into this one
			s.mu.Lock()
			if s.closed {
				// shut down in the meantime: Shutdown already counted this request as abandoned
				s.mu.Unlock()
				return
			}
			if s.queued[r.key()] == r {
				delete(s.queued, r.key())
				s.m().queueLength.Dec()
			}
			s.running[r.key()] = nil
			keep := slices.Clone(r.keep)
			s.mu.Unlock()
			s.m().running.Inc()
			s.process(r.ctx, r.spaceID, r.nodeID, keep)
			s.m().running.Dec()
			s.mu.Lock()
			again := s.running[r.key()]
			delete(s.running, r.key())
			s.donePending(1)
			if again == nil || s.closed {
				// done, or shut down, which abandoned a request to queue again
				s.mu.Unlock()
				continue
			}
			// queue the node once more, at the end of the queue, so that the other nodes get
			// their turn; sending in a goroutine of its own, as a worker must never wait for
			// room in the queue it empties. QueueLength and pending already count the request.
			s.queued[again.key()] = again
			s.mu.Unlock()
			go s.send(again)
		}
	}
}

// cleanupNode reads the node again and cleans it up, it runs in a worker.
func (s *Service) cleanupNode(ctx context.Context, spaceID, nodeID string, keep []string) {
	n, err := node.ReadNode(ctx, s.lu, spaceID, nodeID, "", false, nil, false)
	if err != nil {
		appctx.GetLogger(ctx).Error().Err(err).Str("spaceid", spaceID).Str("nodeid", nodeID).Msg("revision cleanup: could not read node")
		s.reportResult(Result{SpaceID: spaceID, NodeID: nodeID, DryRun: s.dryRun, Err: fmt.Errorf("could not read node: %w", err)})
		return
	}
	if !n.Exists {
		// deleted in the meantime, its revisions went with it (or into the trash)
		s.reportResult(Result{SpaceID: spaceID, NodeID: nodeID, DryRun: s.dryRun, Skipped: "the file no longer exists"})
		return
	}
	s.clean(ctx, n, keep)
}

// clean runs the cleanup of n, or only decides about it in a dry run, and reports the Result.
func (s *Service) clean(ctx context.Context, n *node.Node, keep []string) {
	var result Result
	if s.dryRun {
		result = s.cleaner.DryRun(ctx, s.lu, s.rs, n, keep...)
	} else {
		result = s.cleaner.Cleanup(ctx, s.lu, s.rs, n, keep...)
	}
	s.reportResult(result)
}

// m returns the metrics this Service updates: the exported ones, unless it is the Service of an
// on-demand Run.
func (s *Service) m() *serviceMetrics {
	if s.metrics != nil {
		return s.metrics
	}
	return &driverMetrics
}

func (s *Service) reportResult(result Result) {
	if s.report != nil {
		s.report(result)
	}
}

// addPending counts a new request, s.mu must be held.
func (s *Service) addPending() {
	if s.pending == 0 {
		s.idle = make(chan struct{})
	}
	s.pending++
}

// donePending counts n requests as finished, s.mu must be held.
func (s *Service) donePending(n int) {
	if n <= 0 {
		return
	}
	s.pending -= n
	if s.pending <= 0 {
		s.pending = 0
		close(s.idle)
	}
}

// Wait waits until no cleanup is queued or running anymore, or until ctx ends, in which case
// it returns ctx's error. Requests that were dropped (queue full policy "drop") or abandoned
// by Shutdown don't keep it waiting. In synchronous mode, and for a nil Service, it returns
// right away, as the cleanups already ran in their callers.
//
// Unlike Shutdown, it doesn't stop anything: it is meant for callers that queue a batch of
// cleanups and need them to be done, e.g. a command cleaning up all files of a space.
// Cleanups queued while it waits are waited for as well, until the Service is idle once.
func (s *Service) Wait(ctx context.Context) error {
	if s == nil || !s.async {
		return nil
	}
	s.mu.Lock()
	idle := s.idle
	s.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown stops the background workers. A cleanup that is running is finished, the ones
// still waiting in the queue are abandoned (and logged): they are picked up again when
// their nodes get their next revision. Blocked callers are released. It returns when the
// workers have stopped, or with ctx's error if ctx ends first.
func (s *Service) Shutdown(ctx context.Context) error {
	if s == nil || !s.async {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.done)
	// abandon the requests still waiting, in the queue or to be queued once more: they won't
	// be processed anymore, and their nodes are cleaned up again when they get their next
	// revision
	abandoned := len(s.queued)
	clear(s.queued)
	for key, r := range s.running {
		if r != nil {
			abandoned++
			s.running[key] = nil
		}
	}
	s.donePending(abandoned)
	s.m().queueLength.Sub(float64(abandoned))
	s.m().workers.Sub(float64(s.workers))
	s.m().queueCapacity.Sub(float64(s.queueSize))
	s.mu.Unlock()

	stopped := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		return ctx.Err()
	}

	if abandoned > 0 {
		s.log.Info().Int("abandoned", abandoned).Msg("revision cleanup: stopped, abandoned the cleanups still waiting in the queue")
	}
	return nil
}
