# Abandoned-cart recovery

A cart is abandoned when a shopper edits it and then goes quiet for 30 minutes.
This pipeline detects that, schedules reminders 30 minutes, 1 hour and 24 hours
later, and stops the moment the cart is purchased, cleared or resumed.

It is a take-home case study: the detection-and-scheduling pipeline runs in
process on a fake clock, with in-memory adapters. Nothing is sent.

**Read [docs/DESIGN.md](docs/DESIGN.md) first.** It is the primary artifact.
Supporting detail (full assumptions list, capacity estimate, benchmark numbers,
more rejected alternatives) is in [docs/APPENDIX.md](docs/APPENDIX.md).

## Run it

Needs Go 1.27. No dependencies, no services.

```
make demo        # the 10 end-to-end scenarios, printed as timelines
make test-race   # all tests under the race detector
make bench       # engine hot-path benchmark
make lint        # go vet, gofmt, staticcheck if installed
```

`make demo` output looks like this (`=>` marks an outcome the pipeline recorded):

```
=== RUN   TestPurchaseCancelsRemainingReminders
    00:00         ItemAdded(A) id=e1
    00:20         ItemUpdated(A) id=e2
    00:50         => Abandoned(A, gen=2)
    01:20         => Trigger(A, offset=30m)
    01:25         OrderPlaced(A) id=e3
--- PASS: TestPurchaseCancelsRemainingReminders (0.00s)
```

Each scenario in [test/e2e/scenarios_test.go](test/e2e/scenarios_test.go) is
written as the timeline it verifies:

```go
s.at("00:00", events.ItemAdded, "A").
    at("00:20", events.ItemUpdated, "A").
    advanceTo("00:50").expect("Abandoned(A, gen=2)").
    advanceTo("01:20").expect("Trigger(A, offset=30m)").
    at("01:25", events.OrderPlaced, "A").
    advanceTo("72:00").expect()          // nothing more, ever
```

## Shape

```
Ingest -> log -> engine -> store <- scheduler
                             |
                           relay -> order check -> notifier (stub)
```

| Package | Role |
|---|---|
| `internal/engine` | Cart state machine. Detects abandonment, schedules reminders. |
| `internal/store` | The one transactional store: carts, seen events, timers, outbox, journal. |
| `internal/scheduler` | Fires durable timers from the store, driven by the clock. |
| `internal/delivery` | Outbox relay: pre-send checks, retry with backoff, dead letters, replay, holdout. |
| `internal/clock` | `Clock` interface, wall clock, deterministic fake. |
| `internal/pipeline` | Wires the above in one process. |
| `internal/notifier` | Recording stubs for the notification and order services. |

## Key decisions

- **Event-driven detection** with one durable timer per cart, re-armed when it
  fires, so timer writes scale with carts and not with events.
- **Cancellation by generation.** Every applied cart event bumps the cart's
  generation; older timers and reminders become no-ops. Conflicting-user
  events are recorded and ignored. Nothing is deleted.
- **One transactional store**, so a state change commits together with the
  timers and outbox rows it produces.
- **Idempotent at every hop:** event id into state, `cartId:generation:offset`
  into the outbox and on to the notification service.
- **Delay, not drop, under load**, bounded by a staleness limit past which a
  reminder is suppressed and recorded.

## Requirement traceability

| Requirement | Design | End-to-end timelines (`test/e2e`) | Unit tests, by area |
|---|---|---|---|
| 1. Success and guardrail metrics, control group | [§3](docs/DESIGN.md#3-success-and-guardrail-metrics-requirement-1) | `TestHoldoutIsRecordedButNotSent` | `TestInHoldoutSplit` |
| 2. Detection: event-driven or batch | [§5](docs/DESIGN.md#5-detection-event-driven-not-batch-requirement-2) | `TestHappyPathThreeReminders`, `TestEditResetsTheClock` | `TestTransitions`, `TestEditResetsClockWithOneTimerPerCart`, `TestAbandonmentSchedulesRemindersFromAbandonmentTime` |
| 3. Mid-flight cancellation | [§6](docs/DESIGN.md#6-mid-flight-cancellation-requirement-3) | `TestPurchaseCancelsRemainingReminders`, `TestOrderPlacedBetweenFireAndSendIsCaught`, `TestClearCancelsThenNewEditStartsNewCycle`, `TestResumeCancelsAndReArmsThenNewCycle` | Generation and pre-send checks: `TestReminderForAnOldGenerationIsANoOp`, `TestPreSendChecks`, `TestCartChangeDuringTheOrderCheckCancelsTheSend`. Identity: `TestCartIdentityFollowsTheShopperAcrossLogin`, `TestConflictingUserCannotCancelTheOwnersReminders` |
| 4. Idempotency, no duplicate reminders | [§7](docs/DESIGN.md#7-idempotency-and-delivery-requirement-4) | `TestDuplicateEventDeliveryHasNoEffect`, `TestRestartAndLogReplaySendNothingTwice` | Keys and redelivery: `TestTriggerKey`, `TestDuplicateTimerFiringEnqueuesOnce`, `TestLostAckIsRedeliveredAndDroppedByKey`, `TestTwoRelaysSendingTheSameRowAreDeduplicatedByKey` |
| 5. Failure handling: retry, dead-letter, replay | [§8](docs/DESIGN.md#8-failure-handling-requirement-5) | `TestSendFailuresRetryDeadLetterAndReplay` | Retry budget and deadline: `TestRetriesRideOutAShortOutage`, `TestRetriesStopAtTheStalenessBound`, `TestRetryAtTheDeadlineIsAllowedAndOneTickPastIsNot`, `TestFailureOnTheCapAttemptIsDeadLetteredInsideTheBoundAndSuppressedAfter`, `TestAttemptCapDeadLetters`. Slow calls: `TestSlowOrderCheckCannotPushASendPastTheStalenessBound`, `TestSlowSendStartedInsideTheBoundIsStampedWhenItReturns`, `TestOrderServiceDownFailsClosed`. Replay: `TestLateReplayIsSuppressedNotSent`. Timers and scheduler: `TestFailedTimerIsRedelivered`, `TestRestartedSchedulerResumesFromStore`, `TestReusedTimerIDKeepsDueOrder` |

The unit columns list the tests that carry each requirement, not every test.
The rest are mechanics (the fake clock, backoff arithmetic, config and event
validation, store atomicity, scheduler stop) and are named for what they check.

End-to-end tests are in `test/e2e`; the rest sit beside the package they test.

## Known limitations

- **In memory, one process.** The store's atomicity is real (buffered writes,
  one lock) but its durability is simulated: a "restart" rebuilds the engine,
  scheduler and relay over the same store object.
- **No partitioning.** The store's lock serialises every engine write, from
  log events and timer fires alike; the pipeline mutex only orders log
  consumption. Partitions, consumer lag and timer fires routed through the log
  are designed (DESIGN §4), not built.
- **Head-of-line blocking.** One persistently failing timer blocks every timer
  behind it. Production uses per-shard leases and quarantines a timer after N
  consecutive failures, journalled (DESIGN §8). Not built.
- **No load test.** The only measured number is the engine micro-benchmark.
  Capacity figures (Appendix B) are labelled estimates.
- **Dedupe records never expire.** Retention is specified (DESIGN §7), not
  implemented.
- **Not built:** guest merge, frequency cap, quiet hours, message variants.
  Each is discussed in DESIGN §9.
- **Assignment across guest login is not sticky.** The code switches from
  session id to user id; production should persist assignment against a stable
  first-party identity if treatment must survive login (DESIGN §9).
- CI (`.github/workflows/ci.yml`: lint with staticcheck, then race tests) runs
  on GitHub for every push. It also smoke-runs the engine benchmark for one
  iteration, which only proves it compiles and runs and measures nothing; use
  `make bench` for real numbers. It does not run the demo, since the race tests
  already run the same end-to-end scenarios.
