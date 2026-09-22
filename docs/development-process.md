# Development process and AI use

I used both Claude and ChatGPT while building this assignment, and I want the
division of work to be explicit.

I started from the written SKAO brief and asked Claude to propose an
architecture. That first design established several ideas that remain in the
final system: separate `Observation` and `ProcessingRun` state, a reconciliation
loop for restart recovery, a runner abstraction for fake versus container
execution, reservation-based admission, and rename-before-delete for safer raw
data removal.

I then asked ChatGPT to challenge that design and to turn the agreed design and
the assignment specification into working code and tests. ChatGPT wrote the
controller implementation under my direction, not just a small prototype. I
used Claude again for adversarial reviews of successive versions.

Where the two tools disagreed, I made the decision after checking the failure
mode or running the code. A few examples are useful:

- An early storage-sizing idea relied on a rolling estimate. I rejected that for
  the hard storage limit because an average cannot guarantee a cap. The final
  controller reserves a conservative configured amount before an observation
  starts and separately watches for reservation overrun.
- Unknown raw filesystem data was changed from something that could be cleaned
  automatically to something that is quarantined. I did not want recovery code
  destroying data it could not identify.
- I kept a concrete image-preview path in the QA workflow rather than treating
  all QA-view work as optional UI polish, because the brief explicitly requires
  a person to look at the generated image before raw data can be released.
- The default automatic processing budget was reduced to one execution for this
  exercise. Repeating a deterministic application failure is not useful; a new
  attempt is instead an explicit human reprocess decision.

I did not accept review comments just because an AI suggested them. Findings
were reproduced against the implementation before I changed the design. That
process found real issues in early versions, including transaction boundaries
around QA, accounting for quarantined bytes, interrupted-run recovery, and an
O(history) retry scan.

The part I relied on most heavily for final decisions was direct execution. I
ran the supplied Docker image on my machine, measured the generated Measurement
Set, exercised failure cases, and drove the controller through the full
observe-process-QA-reprocess-delete cycle. Two results changed how I described
the system: the generated MS was about 628 MB on the tested machine rather than
the brief's approximate 1.2 GB, and repeated WSClean runs were numerically very
close but not bit-for-bit identical.

My release rule is that a submission commit is not frozen just because the code
looks plausible or a review says it is ready. The tests must pass, the important
CLI paths must be exercised, the remote CI result must be green, and the final
repository must be checked from a fresh clone.
