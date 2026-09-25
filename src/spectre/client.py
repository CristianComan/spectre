from __future__ import annotations

import asyncio
import logging

from google.protobuf.json_format import MessageToDict

from .ew import EWDetectionSource
from .framing import encode_frame, read_frame
from .messages import (
    build_ew_detection,
    build_registration,
    build_status,
    build_task_ack,
    load_registered_modes,
)
from .proto import SapientMessage, Task, TaskAck

LOG = logging.getLogger("spectre")

# TaskAck.reason strings, taken verbatim from the SAPIENT C-UAS Implementation
# Guide's (references/SAPIENT_IG_V0_4_DRAFT.pdf) Appendix B reserved values,
# so a Fusion Node following the same guide can match on them reliably.
TASK_REASON_UNSUPPORTED_COMMAND = "unsupported command"
TASK_REASON_UNSUPPORTED_MODE = "unsupported mode"
TASK_REASON_CONCURRENT_TASK_LIMIT = "concurrent task limit"
TASK_REASON_RESOURCE_UNAVAILABLE = "resource unavailable"


class SapientEdgeClient:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.node_id = cfg["node"]["node_id"]
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._shutdown = asyncio.Event()

        ew_cfg = cfg.get("ew_detection", {})
        self.ew_source = EWDetectionSource(ew_cfg) if ew_cfg.get("enabled", False) else None

        self.valid_modes, self.default_mode = load_registered_modes(cfg["node"]["registration_file"])
        self.current_mode = self.default_mode
        self.active_task_id: str | None = None

    async def connect(self) -> None:
        fn = self.cfg["fusion_node"]
        LOG.info("Connecting to %s:%s", fn["host"], fn["port"])
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(fn["host"], int(fn["port"])),
            timeout=float(fn.get("connect_timeout_s", 5.0)),
        )
        LOG.info("Connected")

    async def send(self, msg: SapientMessage) -> None:
        if self.writer is None:
            raise RuntimeError("Not connected")
        payload = msg.SerializeToString()
        self.writer.write(encode_frame(payload))
        await self.writer.drain()
        LOG.info(
            "TX %s node_id=%s bytes=%d",
            msg.WhichOneof("content"),
            msg.node_id,
            len(payload),
        )

    @staticmethod
    def _log_rx(msg: SapientMessage, payload_len: int) -> None:
        LOG.info("RX %s node_id=%s bytes=%d", msg.WhichOneof("content"), msg.node_id, payload_len)
        LOG.debug("RX body=%s", MessageToDict(msg, preserving_proto_field_name=True))

    async def receive_loop(self, conn_lost: asyncio.Event) -> None:
        assert self.reader is not None
        try:
            while not conn_lost.is_set():
                payload = await read_frame(self.reader)
                msg = SapientMessage()
                msg.ParseFromString(payload)
                self._log_rx(msg, len(payload))
                if msg.WhichOneof("content") == "task":
                    await self.handle_task(msg.task)
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as exc:
            LOG.warning("Fusion Node connection lost: %s", exc)
            conn_lost.set()

    async def _ack_task(self, task_id: str, status: int, reasons: tuple[str, ...] = ()) -> None:
        try:
            await self.send(build_task_ack(self.node_id, task_id, status, reasons))
        except (ConnectionError, OSError) as exc:
            LOG.warning("Failed to send TaskAck: %s", exc)

    async def handle_task(self, task) -> None:
        """Dispatch an incoming Task; always ends in exactly one TaskAck.

        Scope (see PLAN.md Phase 1): spectre is a fixed, non-pointable EW
        sensor, so only mode_change and request(status|registration) START
        commands are actually implemented; effector/pointable/mobile
        commands (look_at, follow, move_to, patrol, arm) are rejected as
        unsupported.
        """
        LOG.info("Task %s control=%s", task.task_id, task.control)
        if task.control == Task.CONTROL_START:
            await self._handle_task_start(task)
        elif task.control in (Task.CONTROL_STOP, Task.CONTROL_PAUSE):
            await self._handle_task_stop(task)
        else:
            await self._ack_task(task.task_id, TaskAck.TASK_STATUS_REJECTED, (TASK_REASON_UNSUPPORTED_COMMAND,))

    async def _handle_task_start(self, task) -> None:
        which = task.command.WhichOneof("command")
        if which == "mode_change":
            mode = task.command.mode_change
            if mode not in self.valid_modes:
                await self._ack_task(task.task_id, TaskAck.TASK_STATUS_REJECTED, (TASK_REASON_UNSUPPORTED_MODE,))
                return
            if self.active_task_id not in (None, task.task_id):
                await self._ack_task(
                    task.task_id, TaskAck.TASK_STATUS_REJECTED, (TASK_REASON_CONCURRENT_TASK_LIMIT,)
                )
                return
            self.current_mode = mode
            self.active_task_id = task.task_id
            await self._ack_task(task.task_id, TaskAck.TASK_STATUS_ACCEPTED)
        elif which == "request":
            await self._handle_task_request(task)
        else:
            await self._ack_task(task.task_id, TaskAck.TASK_STATUS_REJECTED, (TASK_REASON_UNSUPPORTED_COMMAND,))

    async def _handle_task_request(self, task) -> None:
        request = task.command.request.strip().lower()
        if request == "status":
            await self._ack_task(task.task_id, TaskAck.TASK_STATUS_ACCEPTED)
            await self.send(build_status(self.node_id, self.cfg, self.current_mode, self.active_task_id))
        elif request == "registration":
            await self._ack_task(task.task_id, TaskAck.TASK_STATUS_ACCEPTED)
            await self.send_registration()
        else:
            await self._ack_task(task.task_id, TaskAck.TASK_STATUS_REJECTED, (TASK_REASON_UNSUPPORTED_COMMAND,))

    async def _handle_task_stop(self, task) -> None:
        # STOP and PAUSE both revert the active mode_change task here; spectre
        # relies on the fusion node to resend the task definition on resume
        # rather than caching a paused definition locally.
        if task.task_id != self.active_task_id:
            await self._ack_task(task.task_id, TaskAck.TASK_STATUS_REJECTED, (TASK_REASON_RESOURCE_UNAVAILABLE,))
            return
        self.active_task_id = None
        self.current_mode = self.default_mode
        await self._ack_task(task.task_id, TaskAck.TASK_STATUS_ACCEPTED)

    async def send_registration(self) -> None:
        reg_path = self.cfg["node"]["registration_file"]
        await self.send(build_registration(self.node_id, reg_path))

    async def await_registration_ack(self, timeout: float = 5.0) -> None:
        """Block until the Fusion Node acks (or rejects) our Registration.

        The Fusion Node treats any message arriving before it has finished
        processing Registration as invalid ("unexpected message before
        registration"), so status/detection reporting must not start until
        this returns.
        """
        assert self.reader is not None
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for RegistrationAck")
            payload = await asyncio.wait_for(read_frame(self.reader), timeout=remaining)
            msg = SapientMessage()
            msg.ParseFromString(payload)
            self._log_rx(msg, len(payload))

            kind = msg.WhichOneof("content")
            if kind == "registration_ack":
                if not msg.registration_ack.acceptance:
                    raise RuntimeError(
                        f"Registration rejected: {list(msg.registration_ack.ack_response_reason)}"
                    )
                return
            if kind == "error":
                raise RuntimeError(f"Registration error: {list(msg.error.error_message)}")

    async def status_loop(self, conn_lost: asyncio.Event) -> None:
        interval = float(self.cfg["status"]["interval_s"])
        while not conn_lost.is_set():
            try:
                await self.send(build_status(self.node_id, self.cfg, self.current_mode, self.active_task_id))
            except (ConnectionError, OSError) as exc:
                LOG.warning("Failed to send status report: %s", exc)
                conn_lost.set()
                break
            try:
                await asyncio.wait_for(conn_lost.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def detection_loop(self, conn_lost: asyncio.Event) -> None:
        if self.ew_source is None:
            return
        interval = float(self.cfg["ew_detection"].get("interval_s", 5.0))
        while not conn_lost.is_set():
            try:
                await asyncio.wait_for(conn_lost.wait(), timeout=interval)
            except asyncio.TimeoutError:
                for detection in self.ew_source.sample():
                    try:
                        await self.send(build_ew_detection(self.node_id, detection))
                    except (ConnectionError, OSError) as exc:
                        LOG.warning("Failed to send detection report: %s", exc)
                        conn_lost.set()
                        break

    async def _run_connection(self) -> None:
        conn_lost = asyncio.Event()

        async def watch_shutdown() -> None:
            await self._shutdown.wait()
            conn_lost.set()

        watcher = asyncio.create_task(watch_shutdown(), name="shutdown-watch")
        tasks = [
            asyncio.create_task(self.receive_loop(conn_lost), name="receive"),
            asyncio.create_task(self.status_loop(conn_lost), name="status"),
            asyncio.create_task(self.detection_loop(conn_lost), name="detection"),
        ]

        await conn_lost.wait()

        for task in (*tasks, watcher):
            task.cancel()
        await asyncio.gather(*tasks, watcher, return_exceptions=True)

    async def run(self) -> None:
        reconnect_cfg = self.cfg["fusion_node"].get("reconnect", {})
        initial_delay = float(reconnect_cfg.get("initial_delay_s", 1.0))
        max_delay = float(reconnect_cfg.get("max_delay_s", 30.0))
        delay = initial_delay

        while not self._shutdown.is_set():
            try:
                await self.connect()
                await self.send_registration()
                await self.await_registration_ack()
            except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError, RuntimeError) as exc:
                LOG.warning("Connect failed: %s", exc)
            else:
                delay = initial_delay
                await self._run_connection()
            finally:
                await self.close()

            if self._shutdown.is_set():
                break
            LOG.info("Reconnecting in %.1fs", delay)
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, max_delay)

    async def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass
            LOG.info("Disconnected")
        self.reader = None
        self.writer = None
