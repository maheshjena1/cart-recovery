# Appendix

Detail that supports [DESIGN.md](DESIGN.md). Read it when probing a specific
assumption, number or rejected alternative; the design stands without it.

## A. Assumptions and clarifying questions

Each row is a question I would ask, the default I built on, and what changes if
the answer differs. DESIGN section 2 lists the six that change the design most.

| Question | Assumed default | If wrong |
|---|---|---|
| Are offsets measured from abandonment or from last activity? | From abandonment (last activity + window). First reminder is 60 minutes after the last activity. | Config only: offsets `[0, 30m, 23h30m]` give last-activity anchoring. Zero is a valid offset. |
| Does "resumed" include viewing the cart? | Yes. A view cancels pending reminders and restarts the inactivity clock. | If only edits count, drop `CartResumed` from the activity set. One line. |
| How does the engine learn the cart is empty? | The storefront emits `CartCleared` when the last item is removed. The engine does not track item counts. | A cart emptied by `ItemRemoved` alone would still be reminded. Either fix the producer or give the engine an item count. |
| Is `CheckoutStarted` an outcome? | No. It is activity. An abandoned checkout is the most valuable cart to remind. | If it should stop reminders, treat it like `OrderPlaced`. |
| Which timestamp drives inactivity? | Server ingest time (the log's append time), not the client's `occurredAt`. | Client time would need an out-of-order tolerance rule and clock-skew bounds. |
| Does a cart id survive a purchase? | No. A purchase is terminal; the next journey gets a new cart id. | Reopen on `ItemAdded`, as already done for cleared carts. |
| Is the order service authoritative and queryable by cart id? | Yes. Required for the pre-send check. | Without it the residual race in section 6 widens to the event log's lag. |
| Does the notification service dedupe on an idempotency key? | Yes, for at least the retention in section 7. | Otherwise delivery is at-least-once, with rare duplicates after a lost response. |
| Channels, consent, unsubscribe? | Owned by the notification service. The pipeline sends ids, never addresses. | Consent checks would move into the relay's pre-send checks. |
| Can guests be contacted? | Only if an email or push token was captured; the notification service decides. | No change here; unreachable shoppers are dropped downstream. |
| Per-shopper frequency cap and quiet hours? | Wanted, not built. Section 9. | n/a |
| Retention and privacy? | Cart rows expire 30 days after the last activity. Store holds ids only. | Shorter retention shortens the replay horizon. |

## B. Capacity estimate and benchmark

**Capacity. These are design-time estimates, not measurements.**

- Ingest: 5,000 events/s at roughly 500 bytes is 2.5 MB/s; a 5x spike is
  12.5 MB/s. Well inside one small Kafka cluster.
- Store: one transaction per event, so 5,000 transactions/s at peak. This is
  the bottleneck and the reason to partition the store by cart id.
- Partitions: if one consumer sustains 1,000 to 2,000 transactions/s against
  the store (to be load-tested), peak needs 3 to 5. I would provision 64 for
  spike headroom and fine-grained rebalancing.
- Timers: assuming 6 events per cart and 70% abandonment, peak is about 830
  inactivity timers/s and 1,750 reminder timers/s.

**Measured.** The engine's hot path (dedupe check, transition, one in-memory
commit) takes about 820 ns per event on one core: `BenchmarkHandleEvent`,
811 to 823 ns/op over three runs, 7 allocations per event. Machine: Intel Core
i5-9600K 3.7 GHz, 6 cores, macOS (Darwin 24.6.0), Go 1.27.1. Command:
`make bench`. This shows only that the state machine's CPU cost is negligible
beside store I/O. It says nothing about production throughput.

**Experiment sizing (illustrative, not measured).** With `p = 10%`, `d = 1`
point, `h = 10%`, the formula in section 3 gives about 78,400 shoppers; at
`h = 50%`, about 28,000.

## C. More rejected alternatives

DESIGN section 11 has the decisions a reviewer is most likely to probe. These
are the rest.

| Decision | Rejected | Why |
|---|---|---|
| One timer per cart, re-armed on fire | A timer per event | Timer writes scale with carts, not events |
| Server ingest time | Client `occurredAt` | No clock skew, no out-of-order rule; replay is deterministic |
| Dedupe by event id, per transaction | Per-cart sequence high-water mark | A high-water mark drops a late `OrderPlaced` |
| Holdout as the last pre-send check | Filter held-out shoppers at ingest | Would-have-sent records must mirror real sends exactly |
| Dead letters as an outbox status | Separate queue only | Replay is a status change in the same store; no cross-system move |
| Timer store: a table polled by sharded workers | Redis sorted set; in-memory timing wheel | Redis is a second system, so state and timer cannot commit together. A timing wheel is the most efficient but loses everything on restart unless rebuilt from a durable source, which is the timer table again. |
