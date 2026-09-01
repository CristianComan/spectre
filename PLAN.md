# SPECTRE Implementation Plan

Status snapshot (2026-09-01, updated): v0.1 connects with reconnect-with-
backoff, completes the Registration/RegistrationAck handshake, and sends
periodic StatusReport + multi-emitter EW/RF DetectionReports (simulated,
via `ew.EWDetectionSource`) over BSI Flex 335 v2. Verified clean
(warning-free) against a real Fusion Node — see the registration-validation
quirks in [CLAUDE.md](CLAUDE.md). This plan sequences the remaining
"not yet implemented" items into concrete, buildable phases.

## Guiding constraints

- Stay on **BSI Flex 335 v2.0** (`src/sapient_msg/bsi_flex_335_v2_0`) unless
  a phase explicitly says otherwise. `SAPIENT-Proto-Files-7.0/` (plain
  SAPIENT v7, different package) is a separate protocol variant — do not
  merge its proto package into `src/sapient_msg`.
- `references/Edgenode_Standalone/` is a pattern reference only (older
  proto package, Python 3.8, Flask-based). Port *behavior*, not code.
- Keep the existing async-loop-per-concern shape in `client.py`
  (`receive_loop`, `status_loop`, `detection_loop`, ...). New capabilities
  should add a loop or extend `receive_loop`'s dispatch, not a new
  concurrency model.
- Every new outbound message type gets a `build_*` function in
  `messages.py`, mirroring `build_registration`/`build_status`.

## Phase 1 — Tasking (receive Task, send TaskAck, mode changes)

This is the highest-value gap: without it the node is receive-only and a
real Fusion Node will eventually send a `Task` the client currently just
logs and drops.

1. `messages.py`: add `build_task_ack(node_id, task_id, status, reasons=())`.
2. `client.py`: extend `receive_loop` to dispatch on
   `msg.WhichOneof("content")`:
   - `"task"` -> hand off to a new `handle_task(task_msg)` method.
3. Implement `handle_task`:
   - Inspect `task.WhichOneof("command")` (mirrors
     `references/Edgenode_Standalone/RFsensorEdgeNode.py:347` `Task_handler`,
     but against the BSI Flex v2 `Task` schema in
     `src/sapient_msg/bsi_flex_335_v2_0/task_pb2.py`, not the plain-v7 one).
   - Support at minimum: mode-change tasks (switch `self.current_mode`,
     used by `status_loop`'s `mode` field) and region/request tasks that
     can be acknowledged but not yet acted on.
   - Always reply with `TaskAck` (`TASK_STATUS_ACCEPTED` /
     `TASK_STATUS_REJECTED` with a reason) — never leave a `Task` un-acked.
   - Track `active_task_id` and surface it on `StatusReport` (Flex v2 has
     an equivalent field to the plain-v7 `active_task_id` seen in the
     reference implementation) — check the generated `status_report_pb2`
     for the exact field name.
4. Tests: extend `tests/` with a fake reader/writer pair (or an in-process
   `asyncio` loopback server) to assert Task -> TaskAck round-trip and mode
   change propagation into the next StatusReport.

## Phase 2 — Real RF detection input (replace the EW simulator)

Status: the `Detection`/`build_ew_detection` split described below already
exists (`src/spectre/ew.py` + `messages.build_ew_detection`), but
`EWDetectionSource.sample()` is a synthetic multi-emitter simulator, not a
real detector. What's left:

1. Add a pluggable detector source interface (`async def detections() ->
   AsyncIterator[Detection]`, reusing the existing `ew.Detection`
   dataclass shape) so Phase 3 (Pluto SDR) can implement it without
   touching `client.py`. `EWDetectionSource` can become one implementation
   of this interface (kept as the `source: simulated` dev/test mode —
   useful for protocol-level testing against a Fusion Node without
   hardware) alongside a `source: pluto` implementation later.
2. Ship one non-hardware real-data implementation first (e.g. reading
   pre-recorded IQ/CSV fixtures) to validate the pipeline end-to-end
   before real hardware is involved.
3. Decide `location` vs `range_bearing` per source capability (SDR without
   direction-finding reports `range_bearing` with bearing-only + no range,
   if the v2 schema allows partial population — check
   `range_bearing_pb2.py`; otherwise keep the fixed/jittered-location
   fallback and flag it clearly in logs, consistent with the README's
   existing disclaimer).
4. Remember: any new `Signal`/`classification` field a new source
   populates must also be advertised in `registration.json` (see the
   Fusion Node quirks section in CLAUDE.md) or the Fusion Node will
   silently strip it.

## Phase 3 — Pluto SDR input

1. New module `src/spectre/sdr/pluto.py` wrapping `libiio`/`pyadi-iio`
   (add as an optional dependency group in `pyproject.toml`, e.g.
   `[project.optional-dependencies] sdr = [...]`, so the core client stays
   installable without SDR hardware/drivers).
2. Implement the detector-source interface from Phase 2: stream IQ from
   the Pluto, run whatever detection/energy-threshold logic is in scope
   (start simple — energy detection over configured frequency
   sweep/dwell), yield `Detection` objects.
3. Config: add a `pluto:` section to `spectre.yaml` (device URI, centre
   frequency/sweep plan, sample rate, gain, threshold).
4. This phase is the first one that needs real hardware-in-the-loop
   testing — keep it behind a config flag (`source: synthetic|pluto`) so
   CI/unit tests never require a physical Pluto.

## Phase 4 — SigMF

1. Add SigMF recording of raw/processed IQ alongside live detection
   reporting — `sigmf` Python package, optional dependency.
2. Each `DetectionReport` that originates from real RF should reference or
   be traceable to the SigMF recording window that produced it (dataset +
   annotation), for later offline validation against the Fusion Node log.
3. This is independent of Phases 1–3 in principle but only becomes
   meaningful once Phase 3 (or any real IQ source) exists — sequence it
   after Phase 3 unless SigMF capture is needed earlier for bench testing.

## Phase 5 — SAPIENT-X extensions / EW-specific messages

1. Scope this phase only once the target SAPIENT-X extension spec is
   pinned down (it isn't in this repo yet — check `SAPIENT-Proto-Files-7.0/`
   and any future SAPIENT-X proto drop before starting; do not guess the
   schema).
2. EW-specific message types (beyond generic RF `Signal` detections) will
   likely need new `.proto` extensions or a distinct message package —
   confirm whether these live under BSI Flex 335 v2 or a separate ICD
   version before extending `src/sapient_msg`.
3. Treat this phase as research-then-implement, not implement-directly —
   the schema dependency makes early code risky to commit to.

## Cross-cutting / do alongside whichever phase is active

- **Config validation**: `config.py` currently does zero validation
  (`yaml.safe_load` straight into a dict). Once Tasking (Phase 1) and SDR
  (Phase 3) add new required config keys, add fail-fast validation with
  clear error messages — still no need for a full schema framework at this
  size.
- **Reconnect/resilience**: done — `client.run()` reconnects with
  exponential backoff (`fusion_node.reconnect` config) whenever a
  connection is lost or a connect attempt fails, verified against both a
  real Fusion Node and a disposable flaky TCP server. Revisit only if
  field deployment needs something more (e.g. jittered backoff, max
  retry count/alerting) — not needed for current scope.
- **Tests**: grow `tests/` alongside each phase — Phase 1 in particular
  should not land without a Task -> TaskAck test, since it's the first
  bidirectional protocol behavior in the client. `tests/test_ew.py` is the
  precedent for testing the detection pipeline with a seeded/fake RNG
  rather than real randomness.

## Suggested order

1. Phase 1 (Tasking) — unlocks realistic interop testing with any real
   Fusion Node/Apex, highest value for lowest schema risk.
2. Phase 2 (real detection input, still non-hardware) — de-risks the
   detector-source abstraction before hardware is involved.
3. Phase 3 (Pluto SDR) — first hardware dependency.
4. Phase 4 (SigMF) — layers on top of whatever IQ source exists.
5. Phase 5 (SAPIENT-X/EW) — gated on spec availability, do last.
