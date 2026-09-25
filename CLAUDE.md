# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

SPECTRE is a minimal Python **SAPIENT Edge Node client** implementing plain
**BSI Flex 335 v2.0** (the UK Dstl SAPIENT protocol variant). It connects
over TCP to a SAPIENT Fusion Node / Apex child endpoint, registers itself,
and sends periodic status and (synthetic) detection reports.

This codebase was originally scaffolded with ChatGPT and is now being taken
over for ongoing development in Claude Code. Treat `src/spectre` as the
real product code — it's small, deliberately minimal, and every module has
a single clear job. Don't add abstraction ahead of the roadmap below.

## Repository layout

```
src/spectre/            Product code (async TCP client)
  main.py               CLI entrypoint (argparse -> load_config -> asyncio.run)
  client.py             SapientEdgeClient: reconnect-with-backoff loop wrapping
                         connect / send / receive_loop / status_loop / detection_loop
  config.py             YAML config loader
  framing.py            4-byte LE length-prefix framing over the TCP stream
  messages.py           Builds SapientMessage envelopes (Registration/StatusReport/DetectionReport)
  ew.py                 EWDetectionSource: simulated multi-emitter RF/EW population (see below)
  proto.py              Re-exports generated protobuf classes (raises a clear error if ungenerated)
  ids.py                ULID generation for report_id/object_id

src/sapient_msg/        Generated protobuf Python bindings (output of scripts/compile_protos.sh)
  bsi_flex_335_v2_0/    v2.0 message set actually used by this client

config/
  spectre.yaml          Runtime config: fusion_node host/port, node_id, status/detection intervals
  registration.json     The Registration message body sent on connect (node capabilities/modes)

scripts/
  fetch_protos.sh        git-clones dstl/SAPIENT-Proto-Files into vendor/ (regenerable, gitignored)
  compile_protos.sh      Runs grpc_tools.protoc to regenerate src/sapient_msg/**_pb2.py

tests/                  pytest unit tests (currently just framing)
vendor/                 Fetched upstream proto sources — gitignored, regenerate via fetch_protos.sh
```

### Folders kept out of git on purpose

Two folders exist locally for reference but are **gitignored** (see `.gitignore`)
and must never be committed:

- `references/` — hand-maintained reference material, notably
  `references/Edgenode_Standalone/`, an older, fuller reference Edge Node
  implementation (Flask + Task handling + mode changes + detection file
  logging). Useful as a pattern source when implementing Tasking, but it
  targets an older/plain SAPIENT proto package (`sapient_msg.*`, not the
  BSI Flex 335 namespaced one) and Python 3.8 — don't copy it wholesale.
- `SAPIENT-Proto-Files-7.0/` — a hand-dropped copy of the **plain SAPIENT
  v7.0** proto definitions (package `sapient_msg`, no BSI Flex namespacing,
  no `field_options`/mandatory annotations). This is a *different* protocol
  variant from the BSI Flex 335 v2.0 this client currently speaks — do not
  mix the two proto packages in `src/sapient_msg`. Only pull from here if a
  future task explicitly targets plain SAPIENT v7 rather than BSI Flex 335.

When you need to consult either folder, read it locally — do not assume its
contents are visible to anyone who clones this repo.

## Protocol / wire format

- Transport: plain TCP to `fusion_node.host:fusion_node.port` (config).
- Framing: 4-byte **little-endian** `uint32` payload length, then a
  serialized `SapientMessage` protobuf. No delimiter, no text framing.
- Every `SapientMessage` carries `timestamp`, `node_id` (stable UUID from
  config), and exactly one `oneof content` (`registration`, `status_report`,
  `detection_report`, ...).
- Plain BSI Flex 335 v2 `DetectionReport` requires `location` **or**
  `range_bearing` — this client always fills `location` (WGS84 lat/lon/alt),
  see `messages._set_wgs84_location`. EW detections use jittered synthetic
  locations around a reference point; this is explicitly not a real ESM
  geolocation result (see README "Important plain-v2 limitation").
- Handshake is strict and stateful: after sending `Registration`, the
  client **must wait for `RegistrationAck`** before sending anything else
  (`client.await_registration_ack`) — the reference Fusion Node used for
  conformance testing here (see below) rejects any message that arrives
  before it has finished processing Registration, even ones that are
  technically ordered correctly on the wire but arrive close together. The
  same node also needs the first `StatusReport` to land before it will
  accept `DetectionReport`s ("unexpected message before initial status");
  the periodic `status_loop`/`detection_loop` timing already gives this
  enough of a gap in practice, but don't assume you can fire a detection
  before status on a freshly opened connection.

### Fusion Node registration-validation quirks (non-obvious, verified against a real node)

The Fusion Node used for local testing here is Sapient's `CI-map-viewer`
(a Java/Clojure SAPIENT validator + map viewer, found listening on
`127.0.0.1:5020`). Its validator is strict about things the proto schema
itself doesn't enforce. Two gotchas worth knowing before touching
`registration.json` or `messages.py`:

1. **`icdVersion` is checked against an exact string**, not just any
   non-empty text: `"BSI Flex 335 v2.0 / NATO STANREC 4869 Ed A V1.0"`
   (found by reading the validator's compiled classes — see
   `SapientMessageValidator.class` strings — since the schema itself
   places no constraint on this field). Getting this wrong produces
   `error: unsupported protocol version: <whatever you sent>` and the
   connection is closed immediately, before any other validation runs.
2. **Every optional `DetectionReport`/`Signal`/`classification` field you
   actually populate must be individually advertised in the active mode's
   `detectionDefinition.detectionReport[]` / `.detectionClassDefinition[]`
   in `registration.json`, using a name the validator recognizes** — it's
   not enough to have "a" `DETECTION_REPORT_CATEGORY_SIGNAL` entry. The
   validator normalizes both the registration `type` string and the
   internal field name via `lowercase + strip everything but [a-z0-9]`,
   then does a set-membership check per field. In practice this means:
   `detection_confidence` needs a `DETECTION`-category entry whose `type`
   normalizes to `confidence` (e.g. `"Confidence"`); each `Signal`
   sub-field (`amplitude`, `start_frequency`, `centre_frequency`,
   `stop_frequency`, `pulse_duration`) needs its own `SIGNAL`-category
   entry; and `classification.type` needs a matching `classDefinition`
   entry under `detectionClassDefinition` (trimmed + lowercased exact-text
   match — casing/punctuation don't matter, but the words do). Fields that
   fail this check aren't rejected outright — the connection stays open,
   but the Fusion Node silently **strips the field** and returns a
   `warning: ... field ignored` message, which is easy to miss if you're
   only checking for hard errors.

   **If you add a new `Signal`/`classification` field to an EW emitter in
   `ew.py`/`spectre.yaml`, you must add a matching declaration to
   `registration.json`'s `detectionReport`/`detectionClassDefinition`
   arrays, or the Fusion Node will quietly drop that field.**

   (These were reverse-engineered from the shipped `.class` files with the
   pure-Python `jawa` bytecode reader — no JDK was available locally. If
   you need to re-verify against a different Fusion Node implementation,
   don't assume these exact strings/behavior carry over.)
3. **This particular Fusion Node only accepts node_ids it already knows
   about.** Registering with a fresh/unrecognized UUID gets the TCP
   connection dropped immediately after Registration is sent — no `error`
   message, just a closed socket (confirmed via its log:
   `TCP client connected` followed by nothing, vs. the known default
   node_id which gets a `RegistrationAck` within milliseconds). This is
   presumably a known/trusted-nodes allowlist configured in the Fusion
   Node's own UI/settings, not something `spectre` controls. If a new
   `--node-id` doesn't register, this is the first thing to check —
   it is not necessarily a `registration.json` problem.

## Build / run workflow

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Only needed once, or after a proto update:
./scripts/fetch_protos.sh      # clones dstl/SAPIENT-Proto-Files into vendor/
./scripts/compile_protos.sh    # regenerates src/sapient_msg/**_pb2.py

spectre --config config/spectre.yaml
```

A SAPIENT Fusion Node (e.g. Apex child endpoint) must be listening at the
configured host/port (default `127.0.0.1:5020`) for the client to connect;
otherwise it fails fast on `asyncio.open_connection`.

Tests: `pytest` (currently minimal — expand alongside new features rather
than treating test coverage as optional).

**Node identity**: `node.node_id` in `spectre.yaml` is read once at
startup (`main.py`) and never regenerated — it stays fixed for the whole
process lifetime, including across `client.py`'s automatic reconnects.
One running instance = one stable node_id. `main.py` also accepts
`--node-id <uuid>` to override the config value, which exists solely for
running multiple `spectre` instances in parallel against the same Fusion
Node (each needs its own distinct UUID) — don't use it to rotate a single
instance's identity between runs.

## Conventions to follow

- Async everywhere in `client.py`, with two-level shutdown handling:
  `self._shutdown` is the process-level stop signal (set once, never
  reset); each connection attempt gets its own fresh `conn_lost`
  `asyncio.Event`, and a small watcher task forwards `_shutdown` into
  `conn_lost` so per-connection loops only ever need to check one event.
  `run()` wraps connect -> register -> await ack -> run loops -> close in
  a `while not self._shutdown.is_set()` loop with exponential backoff
  (`fusion_node.reconnect` config), so a dropped connection reconnects
  instead of exiting. Keep new long-running loops (e.g. a future
  task-handling loop) following the existing shape: `while not
  conn_lost.is_set(): ... await asyncio.wait_for(conn_lost.wait(),
  timeout=...)`, and have them set `conn_lost` (not raise) on a send/recv
  failure so the outer reconnect loop notices.
- All outbound messages are built as small, pure `build_*` functions in
  `messages.py` that return a fully-populated `SapientMessage`, then sent
  via `client.send()`. Keep new message types (TaskAck, Alert, ...)
  following this same builder shape.
- `proto.py` is the single import surface for generated protobuf classes —
  import from there, not directly from `sapient_msg.bsi_flex_335_v2_0.*`,
  so missing-bindings errors stay centralized and clear.
- Config is a plain dict from YAML (`config.py`), not a schema/dataclass.
  Don't introduce a config framework without discussing it first — the
  project is intentionally minimal at v0.1.
- No error handling/fallbacks for cases that can't occur; validate only at
  real boundaries (parsing external config/registration files, the network
  socket).

## Current scope (v0.1) vs. not yet implemented

Implemented: TCP connect with reconnect-with-backoff, BSI Flex 335 v2
framing, Registration -> RegistrationAck handshake, periodic StatusReport,
periodic multi-emitter EW/RF `DetectionReport`s via `ew.EWDetectionSource`
(amplitude/frequency band/classification per emitter, with emitters
independently rolling on/off and persistent per-track `object_id`), YAML
config, TX/RX logging. Phase 1 Tasking is also implemented: receiving
`Task`, dispatching `mode_change`/`request(status|registration)`/
`START`/`STOP`/`PAUSE` (see `client.handle_task` and PLAN.md's "Phase 1 —
Tasking" section for the full design and live-verification notes),
region tasking (accept-and-store, no filtering yet), and observability
(`mode_history.ModeHistory` + `netmon.NetworkStats`, ported from the
sibling `interdictor` repo). Verified end-to-end (clean, warning-free)
against a real Fusion Node (see quirks above), including a real
Fusion-Node-issued Task -> TaskAck round-trip.

Not yet implemented (see [PLAN.md](PLAN.md) for sequencing): region
filtering of detections, Pluto SDR input, SigMF, SAPIENT-X extensions,
real RF detection processing (the EW emitters are still simulated, not
derived from actual RF hardware).

## Git

- `references/`, `SAPIENT-Proto-Files-7.0/`, `vendor/`, `.venv/` are
  gitignored — never `git add -f` them.
- Working branch is `develop`; `main` is the default/PR-target branch.
