# RC1 validation status

This release candidate is the cleaned form of the v5 repository that passed the
final adversarial review.

Validated before RC cleanup:

- 32/32 project tests passed.
- Real supplied Docker image executed successfully on Apple Silicon via amd64 emulation.
- Real storage-backpressure, QA accept, raw-data deletion, automatic observing resume,
  reprocessing, product-history retention, and final cleanup paths were exercised end-to-end.
- Long-campaign scheduler cost remained approximately flat through 1,000 exhausted historical
  failures in the measured development run.

RC1 cleanup changes are intentionally non-architectural:

- removed Python/test cache artefacts;
- ignored `workspace-real/` and `*.egg-info/`;
- made Ctrl-C exit without an asyncio traceback;
- aligned GitLab CI with Python 3.11;
- corrected "reproducible" to "regenerable" for interrupted derived products;
- recorded the real reprocessing numerical-reproducibility measurements.

No state-machine, admission, reconciliation, deletion, retry, or QA release-authority logic
was redesigned in this cleanup.
