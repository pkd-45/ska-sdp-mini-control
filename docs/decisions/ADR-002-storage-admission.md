# ADR-002: Reservation-based admission with physical raw-byte backpressure

The enforceable admission rule is evaluated before a new observation is created:

`logical_used + logical_reserved + quarantine + trash + next_reservation <= threshold`.

`logical_used + logical_reserved <= threshold` remains the controller's logical reservation invariant, while quarantine and trash are additionally charged because they consume the same physical disk and repeated crashes must not bypass backpressure.

Each observation receives a configured conservative reservation before it starts. While it runs, its apparent size is polled. With `reservation_overrun_action: abort`, the controller stops the writer, waits for it to be gone, quarantines the partial MS, and releases the reservation. With `warn`, it records an event and allows completion; this explicitly relaxes the completion property rather than pretending the reservation was a hard upper bound.

Quarantine retention is operator-controlled. The controller never automatically destroys quarantined raw data; `sdpctl quarantine list|purge` makes the retention decision explicit.
