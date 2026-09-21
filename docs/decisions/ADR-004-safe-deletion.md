# ADR-004: Rename before remove

In the normal path, raw visibility deletion occurs only after an accepted QA verdict. An explicit operator discard of an exhausted processing failure is the only additional deletion path, and it is recorded as `DISCARDED`, never `ACCEPTED`.

The MS is first renamed into a trash path on the same filesystem, then recursively removed, then marked `DELETED`. A crash in the middle can be finished deterministically on restart. If both the live and trash copies exist, automatic reconciliation refuses to guess, records the observation as wedged, continues with other entities, and requires explicit operator resolution.
