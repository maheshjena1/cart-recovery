// Package pipeline wires the components into one in-process pipeline:
//
//	Ingest -> log -> engine -> store <- scheduler
//	                             |
//	                           relay -> notifier
//
// It stands in for the production topology, where the log is a topic
// partitioned by cart id and each partition has a single consumer. Here the
// store's lock serialises every write, whether it comes from a log event or
// from a timer fire; the pipeline's own mutex only orders log consumption.
package pipeline

import (
	"math/rand/v2"
	"sync"
	"time"

	"cartrecovery/internal/clock"
	"cartrecovery/internal/config"
	"cartrecovery/internal/delivery"
	"cartrecovery/internal/engine"
	"cartrecovery/internal/events"
	"cartrecovery/internal/scheduler"
	"cartrecovery/internal/store"
)

// Deps are the pipeline's collaborators. Store is the durable part: it
// outlives a Restart. Jitter may be nil.
type Deps struct {
	Clock    clock.Clock
	Store    *store.Store
	Notifier delivery.Notifier
	Orders   delivery.Orders
	Jitter   func() float64
}

// Record is one entry of the event log.
type Record struct {
	Offset     int
	IngestedAt time.Time
	Event      events.Event
}

type Pipeline struct {
	cfg  config.Config
	deps Deps

	mu    sync.Mutex
	log   []Record
	next  int // consumer offset: the first record not yet applied
	eng   *engine.Engine
	relay *delivery.Relay
	sched *scheduler.Scheduler
}

func New(cfg config.Config, deps Deps) (*Pipeline, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if deps.Jitter == nil {
		deps.Jitter = rand.Float64
	}
	p := &Pipeline{cfg: cfg, deps: deps}
	p.start()
	return p, nil
}

// start builds the stateless components over the durable store.
func (p *Pipeline) start() {
	eng := engine.New(p.cfg, p.deps.Clock, p.deps.Store)
	relay := delivery.New(p.cfg, p.deps.Clock, p.deps.Store, p.deps.Notifier, p.deps.Orders, p.deps.Jitter)
	// A fired timer is handled by the engine, then anything it put in the
	// outbox is delivered. A retry timer only needs the delivery pass.
	fire := func(t store.Timer) error {
		if t.Kind != store.Retry {
			if err := eng.HandleTimer(t); err != nil {
				return err
			}
		}
		return relay.Drain()
	}
	p.eng, p.relay, p.sched = eng, relay, scheduler.New(p.deps.Clock, p.deps.Store, fire)
}

// Ingest validates an event, appends it to the log stamped with the ingest
// time, and consumes it.
func (p *Pipeline) Ingest(ev events.Event) error {
	if err := ev.Validate(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log = append(p.log, Record{Offset: len(p.log), IngestedAt: p.deps.Clock.Now(), Event: ev})
	return p.consume()
}

// consume applies log records from the consumer offset on. The offset moves
// only past records that were applied, so a failed record is retried by the
// next call.
func (p *Pipeline) consume() error {
	for p.next < len(p.log) {
		r := p.log[p.next]
		if err := p.eng.HandleEvent(r.Event, r.IngestedAt); err != nil {
			return err
		}
		p.next++
	}
	if err := p.relay.Drain(); err != nil {
		return err
	}
	p.sched.Arm()
	return nil
}

// Restart simulates a crash and recovery: the engine, relay and scheduler are
// thrown away and rebuilt over the same store, and the consumer rewinds to
// fromOffset and reprocesses the log from there.
func (p *Pipeline) Restart(fromOffset int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sched.Stop()
	p.start()
	p.next = fromOffset
	return p.consume()
}

// Replay re-drives one dead-lettered trigger. See delivery.Relay.Replay.
func (p *Pipeline) Replay(key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.relay.Replay(key); err != nil {
		return err
	}
	p.sched.Arm()
	return nil
}
