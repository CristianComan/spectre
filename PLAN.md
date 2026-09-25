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

**Sources for this design**: the BSI Flex 335 v2 `task.proto` /
`task_ack.proto` / `registration.proto` (`TaskDefinition`) schemas under
`vendor/SAPIENT-Proto-Files/bsi_flex_335_v2_0/`, and
`references/SAPIENT_IG_V0_4_DRAFT.pdf` ("SAPIENT C-UAS Implementation
Guide", draft 2026.0.4) §5.3 (message lifecycle), §9 (Tasking Profile),
§9.1 (Rejection Rules), §12 (hierarchical/task decomposition) and
Appendix B (reserved `TaskAck.reason` strings). The IG is C-UAS-flavoured
guidance layered on top of the base BSI Flex standard, not a protocol
change — spectre is a plain RF/EW sensor node, not an effector, so only
the sensor-relevant subset applies (no `REQUEST Arm`, no effector safety
interlocks).

### Schema recap (BSI Flex v2, as actually generated)

- `Task` (`task_pb2.py`): `task_id` (ULID), `task_name`,
  `task_description`, `task_start_time`/`task_end_time`, `control`
  (`Task.Control` enum: `CONTROL_START` / `CONTROL_STOP` / `CONTROL_PAUSE`
  — `CONTROL_DEFAULT` was removed from the schema, don't implement it),
  repeated `region`, and one `command` message. `Task.Command` is a
  `oneof`: `request` (free string), `detection_threshold` /
  `detection_report_rate` / `classification_threshold` (all
  `DiscreteThreshold` low/medium/high), `mode_change` (string mode name),
  `look_at` / `move_to` / `patrol` / `follow` (pointable/mobile-platform
  commands — not applicable to spectre's fixed EW sensor).
- `TaskAck` (`task_ack_pb2.py`): `task_id`, `task_status`
  (`TASK_STATUS_ACCEPTED` / `REJECTED` / `COMPLETED` / `FAILED`), repeated
  `reason` strings, optional `associated_file`.
- `StatusReport.active_task_id` (`status_report_pb2.py`, field 4): ULID of
  the task currently being executed — this is Flex v2's equivalent of the
  plain-v7 reference implementation's `active_task_id`.
- `Registration.ModeDefinition.TaskDefinition` (`registration.proto`):
  `concurrent_tasks`, `region_definition`, and **repeated `command`**
  (each a `{units, completion_time, type: CommandType}` entry, where
  `CommandType` enumerates `REQUEST` / `DETECTION_THRESHOLD` /
  `DETECTION_REPORT_RATE` / `CLASSIFICATION_THRESHOLD` / `MODE_CHANGE` /
  `LOOK_AT` / `MOVE_TO` / `PATROL` / `FOLLOW`).

### Registration gap (must fix before Task testing, per CLAUDE.md quirks)

`config/registration.json`'s `modeDefinition[0].task` currently declares
only `concurrentTasks: 1` and a `regionDefinition` — **no `command` array**.
Per the same validator behavior already documented for detection fields in
[CLAUDE.md](CLAUDE.md) ("every optional field you populate must be
individually advertised or it's silently accepted-but-ignored"), any
`Task.Command` spectre is expected to *accept* should have a matching
`CommandType` entry declared here first. For the MVP scope below, add:

```json
"command": [
  { "type": "COMMAND_TYPE_REQUEST", "units": "status|registration", "completionTime": {"units": "TIME_UNITS_SECONDS", "value": 1.0} },
  { "type": "COMMAND_TYPE_MODE_CHANGE", "units": "MONITOR", "completionTime": {"units": "TIME_UNITS_SECONDS", "value": 5.0} }
]
```

**Verified live against `CI-map-viewer`**: an earlier draft used a
descriptive placeholder (`"units": "registered mode name"`) for the
`COMMAND_TYPE_MODE_CHANGE` entry's `units` field. The live Fusion Node
rejected the whole Registration for it with `error: invalid value:
registration.mode_definition MONITOR MODE_CHANGE expected a registered
mode, found registered mode name` — i.e. `units` on a `MODE_CHANGE`
command entry isn't free text, it must literally be (a) registered mode
name(s) this command can switch into. Fixed to `"MONITOR"` (the only mode
currently registered); revisit once a second mode exists.

### MVP scope (map IG §9's task types onto what spectre can actually do)

spectre is a fixed, non-pointable EW/RF sensor, so of the IG's tasking
table only a subset is meaningful now:

| IG task type | Maps to (BSI Flex v2) | Phase 1 scope |
|---|---|---|
| `MODE_CHANGE` | `Task.Command.mode_change` (string) | **Yes** — switch `self.current_mode`, validate against `registration.json`'s declared mode names, reject if unknown |
| `REQUEST Status` | `Task.Command.request` (string, e.g. `"status"`) | **Yes** — trigger an immediate out-of-cycle `StatusReport` |
| `REQUEST Registration` | `Task.Command.request` (e.g. `"registration"`) | **Yes** — resend `Registration` (rare; the node normally registers once) |
| `START` / `STOP` / `PAUSE` (`Task.control`) | `Task.Control` enum | **Yes** — track one `active_task_id`, honor `concurrent_tasks: 1` from registration (reject a second concurrent task per IG §9.1) |
| Region tasking (`Task.region`) | `Task.region` | **Ack only** — accept and store the region, do not yet filter/gate detections by it (matches PLAN's existing "acknowledged but not acted on" scoping) |
| `detection_threshold` / `detection_report_rate` / `classification_threshold` | `Task.Command.*` (`DiscreteThreshold`) | Defer — meaningful once Phase 2's pluggable detector source exists; for now reject with `unsupported command` |
| `REQUEST Arm`, `LOOK_AT`, `FOLLOW`, `MOVE_TO`, `PATROL` | effector/pointable/mobile commands | **Out of scope** — spectre has no effector or steerable/mobile platform; reject with `unsupported command` |

### Rejection reasons (use IG Appendix B's reserved strings verbatim)

Standardize `TaskAck.reason` on the IG's reserved values so the string
match in `9.1 Rejection Rules` / Appendix B lines up exactly:
`unknown object_id`, `unsupported command`, `unsupported mode`,
`concurrent task limit`, `authority not granted`, `invalid region`,
`resource unavailable`. `build_task_ack` should take a list of these
literal reason strings, not free-form text, so future Fusion Nodes
matching this IG can parse them reliably.

### Implementation steps

1. `messages.py`: add `build_task_ack(node_id, task_id, status, reasons=())`
   mirroring the existing `build_status`/`build_ew_detection` builder shape
   (`_wrap(node_id, "task_ack", body)`).
2. `client.py`: extend `receive_loop` to dispatch on
   `msg.WhichOneof("content")`:
   - `"task"` -> hand off to a new `handle_task(task_msg)` method, which
     sends exactly one `TaskAck` before returning (never leave a `Task`
     un-acked, per IG §5.3's `Task` row and §11's Error-vs-TaskAck
     distinction — a malformed `Task` gets an `Error`, a well-formed but
     unsupported/rejected one always gets `TaskAck`, never both).
3. Implement `handle_task` per the MVP scope table above:
   - `control == CONTROL_START`: inspect `command.WhichOneof("command")`;
     dispatch `mode_change` -> validate against registered mode names,
     switch `self.current_mode`, set `self.active_task_id = task.task_id`;
     dispatch `request` -> trigger immediate status (and/or re-registration)
     send; anything else in the MVP table's "defer"/"out of scope" rows ->
     `TASK_STATUS_REJECTED` with the matching Appendix B reason.
   - `control == CONTROL_STOP`/`CONTROL_PAUSE`: if it matches
     `self.active_task_id`, clear/pause it and revert `self.current_mode`
     to the registered default mode; otherwise reject
     (`unknown object_id`-style "no such active task" — IG doesn't give a
     dedicated reason for this case, reuse `resource unavailable`).
   - Enforce `concurrent_tasks: 1` (from `registration.json`): reject a
     second concurrent `CONTROL_START` with `concurrent task limit` while
     `self.active_task_id` is set, mirroring IG §9.1.
   - Always reply with `TaskAck` (`TASK_STATUS_ACCEPTED` /
     `TASK_STATUS_REJECTED` with reason(s) from the Appendix B set) —
     never leave a `Task` un-acked.
   - Track `self.active_task_id` and surface it on `StatusReport` via the
     confirmed `active_task_id` field (`status_report_pb2.py` field 4).
4. Update `config/registration.json` per the "Registration gap" section
   above, then re-verify end-to-end against the local `CI-map-viewer`
   Fusion Node the same way the original handshake/detection quirks were
   verified (watch for `warning: ... field ignored` on the Task path, not
   just hard errors).
5. Tests: extend `tests/` with a fake reader/writer pair (or an in-process
   `asyncio` loopback server) to assert: Task(`MODE_CHANGE`) -> TaskAck
   `ACCEPTED` -> mode change visible in the next `StatusReport`; a second
   concurrent `CONTROL_START` -> TaskAck `REJECTED` /
   `concurrent task limit`; an out-of-scope command (e.g. `look_at`) ->
   TaskAck `REJECTED` / `unsupported command`; `CONTROL_STOP` on the active
   task -> TaskAck `ACCEPTED` and `active_task_id` cleared on the next
   `StatusReport`.

### Verification status (live `CI-map-viewer` round-trip)

Implemented in `client.py`/`messages.py`/`registration.json` and unit
tested in `tests/test_tasking.py` (8 cases, all passing). Also exercised
against the real local Fusion Node:

- Registration with the new `command` declarations is accepted cleanly
  (after the `units` fix above) — no `warning: ... field ignored` on the
  Task path.
- Two real `Task` messages sent from the Fusion Node's UI were received,
  dispatched, and answered with a `TaskAck` (`control=1` /
  `CONTROL_START` in both cases) — confirms the `receive_loop` ->
  `handle_task` -> `TaskAck` path works end-to-end, not just in the unit
  tests.
- **Resolved**: the `Error` seen after `TaskAck` (both the very first live
  round-trip and the `request: "registration"` path) turned out to be a
  real bug, now fixed. Captured with `logging.level: DEBUG` and a single
  instance: `missing mandatory field: destination_id for task_ack`. The
  `SapientMessage.destination_id` field is optional in the proto schema,
  but the Fusion Node validator requires it on `task_ack` specifically -
  same reverse-engineered-quirk pattern as the other CLAUDE.md entries.
  Fixed by threading the Task's source `node_id` (the enclosing
  `SapientMessage.node_id`, not a field on `Task` itself) through
  `receive_loop` -> `handle_task` -> `build_task_ack`, which now requires
  a `destination_id` argument. Verified clean end-to-end against a real
  Fusion-Node-issued `request: "registration"` Task after the fix: `TaskAck`
  -> `Registration` resend -> `RegistrationAck`, no errors. (A follow-up
  `unexpected message before registration` seen mid-investigation turned
  out to be a false lead from testing with a locally-fabricated task_id
  the Fusion Node had never issued, not a real bug - the real
  Fusion-Node-issued task round-tripped clean.)
- Confirmed operationally: only run **one** `spectre` process per
  `node_id` at a time — the Fusion Node has no fencing for a duplicate
  connection, so two processes with the same `node_id` repeatedly kick
  each other's connection (`0 bytes read on a total of 4 expected bytes`,
  reconnect loop thrashing every ~1-2s). Not a code bug, just an
  operational hazard to remember when testing.

### Region tasking — done (ack-only, per the MVP scope table)

`client._extract_regions` converts an accepted Task's `region` list to
plain dicts via `MessageToDict` and stores them on
`self.active_task_regions`, logging `Task <id> declares N region(s): ...`.
Cleared on `STOP`/`PAUSE` and on reconnect. Still no detection filtering
by region — that's out of scope until a real detector source (Phase 2)
makes region gating meaningful.

### Observability: mode-change history and network stats

Ported from the sibling `interdictor` effector-node repo (same protocol,
opposite role), which built these for the same reason: seeing what's
being sent and how Tasking changes the node's behaviour, without a
separate polling API or dashboard process.

- **`mode_history.ModeHistory`** — every accepted `mode_change` and every
  `STOP`/`PAUSE` reversion calls `client._record_mode_change`, which logs
  a distinct `MODE CHANGE: <from> -> <to> (task_id=...)` line and appends
  a capped (100-entry) `ModeTransition` list. The latest transition is
  passed into `messages.build_status`, which adds it as a
  `StatusReport.status[]` entry (`STATUS_TYPE_OTHER`) — visible on the
  Fusion Node/C2 UI itself, not just local logs. Required a matching
  `registration.json` declaration (`statusDefinition.statusReport[]`,
  `category: STATUS_REPORT_CATEGORY_STATUS`, `type: "Mode Change"`) or the
  Fusion Node silently strips the field — same quirk class as the
  detection-field declarations. A mode-change Task now also triggers an
  immediate out-of-cycle `StatusReport` (via the new `client.send_status`
  helper, which both loops and `handle_task` call), so the behaviour
  change is visible without waiting for the next `status.interval_s` tick.
- **`netmon.NetworkStats`** — counts messages/bytes sent and received (per
  message type), plus send/receive errors, connect attempts/failures, and
  disconnects. Every `send()`/`receive_loop()`/`connect()`/`close()`/`run()`
  call site updates it; `send_status()` logs `net_stats.summary()` once
  per status interval. Counters persist for the whole process lifetime,
  including across reconnects — meant to answer "how has this connection
  behaved overall," not just "since the last reconnect."
- Not ported: `interdictor`'s `TaskAck` builder still doesn't set
  `destination_id` at all — likely the same bug this project just fixed,
  worth checking there separately (out of scope for this repo).

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
