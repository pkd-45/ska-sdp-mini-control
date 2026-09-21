# Real supplied-container probe

The supplied `docker.io/pw410/ska-sdp-mock:0.1` image was executed successfully on an Apple Silicon Mac with Docker Desktop 29.8.0 on 20 September 2026.

The image itself is `linux/amd64`, while the host is `linux/arm64/v8`, so Docker Desktop used architecture emulation and printed the expected platform-mismatch warning. The commands nevertheless completed successfully. These timings are therefore local validation measurements, not performance guarantees for the SKAO deployment platform.

## Measurements

| Check | Result |
|---|---:|
| Visibility generation exit code | 0 |
| Visibility generation wall time | 18.488 s |
| Visibility generation sampled peak container memory | 420,688,691 B |
| Measurement Set apparent size | 628,150,970 B |
| Measurement Set `du` size | 613,704 KiB |
| Processing exit code | 0 |
| Processing wall time | 33.944 s |
| Processing sampled peak container memory | 165,884,723 B |
| Broken-MS processing exit code | 255 |
| Missing output-parent exit code | 255 |
| Same-prefix rerun exit code | 0 |

Processing produced the expected `dirty`, `image`, `model`, `psf`, and `residual` FITS files.

## Design consequences

1. The controller must create the processing output directory before launching WSClean. `ContainerRunner.process()` already does this.
2. Non-zero exit codes are meaningful failure signals; the broken-input case returned 255.
3. Re-running the same output prefix succeeds, but logical human reprocessing still creates a new run directory so history is preserved instead of relying on overwrite semantics.
4. The configured `1300MiB` observation reservation remains deliberately conservative relative to the locally measured ~628 MB MS and to the assignment's approximate-size statement.
5. Processing took about 1.84 times as long as visibility generation in this emulated local run. A demo default of two parallel processing slots is therefore sufficient to show why processing concurrency is useful without making the laptop unnecessarily busy.
6. Sampled memory comes from `docker stats` every 0.2 s. It is a sampled peak, not an exact RSS high-water mark.

## End-to-end real-controller validation

The controller was then exercised end-to-end against the real container with:

- `storage_threshold: 2GiB`
- `observation_reservation: 1300MiB`
- `max_parallel_processing: 2`
- `max_processing_attempts: 1`
- `max_observations: 3`

After observations 1 and 2 completed, the controller held two real Measurement Sets:

- logical raw used: `1,256,301,940 B`
- reserved: `0 B`
- both observations: `STORED`
- both processing runs: `AWAITING_QA`

A third observation was correctly not admitted because adding the configured
`1,363,148,800 B` reservation would have exceeded the `2,147,483,648 B`
threshold.

After visually accepting the first product, observation 1 moved to `DELETED`,
its raw MS was removed, and observation 3 automatically entered `OBSERVING`.
At that point the controller charged:

- retained raw data: `628,150,970 B`
- new reservation: `1,363,148,800 B`
- total charged: `1,991,299,770 B`

Observation 2 was then explicitly reprocessed. Its original raw MS remained
present and charged, run 1 was superseded, run 2 was created at processing
attempt 2, and both generations of derived products were retained for audit
history.

After visually accepting the remaining products, the final controller state
was:

- logical raw used: `0 B`
- reserved: `0 B`
- quarantine: `0 B`
- trash: `0 B`
- charged: `0 B`
- observations 1, 2 and 3: `DELETED`
- QA queue: empty

## Reprocessing reproducibility

The exercise describes reprocessing as re-running the same configuration and
therefore giving the same result. In the real WSClean test, the first and
second processing attempts were **not bit-for-bit identical**: all five FITS
files had different SHA-256 hashes.

Direct array comparisons showed very small numerical differences:

| Product | RMS-relative difference |
|---|---:|
| dirty | ~2.32e-7 |
| image | ~4.01e-7 |
| model | ~6.37e-7 |
| PSF | ~2.90e-7 |
| residual | ~4.81e-7 |

The largest measured pointwise relative difference was about `4.59e-5` in the
residual image. For this exercise the reruns are therefore described as
numerically/scientifically equivalent for the tested case, but not bitwise
reproducible. The controller preserves every logical processing attempt
instead of relying on byte identity.
