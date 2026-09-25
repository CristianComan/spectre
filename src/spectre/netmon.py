from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class NetworkStats:
    """TCP-connection-level counters for observability.

    Wired into client.py's connect/send/receive_loop/status_loop/close, so
    every message exchanged with the Fusion Node - and every connection-level
    error, reconnect and disconnect - is counted. `summary()` renders a
    compact one-line snapshot; client.py logs it once per status interval
    (piggybacking on the existing periodic StatusReport cadence rather than
    adding a second timer loop). The per-message-type counters make it easy
    to spot e.g. a TaskAck that never went out.
    """

    messages_sent: int = 0
    messages_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    sent_by_type: dict[str, int] = field(default_factory=dict)
    received_by_type: dict[str, int] = field(default_factory=dict)
    send_errors: int = 0
    receive_errors: int = 0
    connect_attempts: int = 0
    connect_failures: int = 0
    disconnects: int = 0
    last_error: str | None = None
    last_error_at: datetime | None = None

    def record_sent(self, kind: str, num_bytes: int) -> None:
        self.messages_sent += 1
        self.bytes_sent += num_bytes
        self.sent_by_type[kind] = self.sent_by_type.get(kind, 0) + 1

    def record_received(self, kind: str, num_bytes: int) -> None:
        self.messages_received += 1
        self.bytes_received += num_bytes
        self.received_by_type[kind] = self.received_by_type.get(kind, 0) + 1

    def record_error(self, message: str, *, send: bool = False, receive: bool = False) -> None:
        if send:
            self.send_errors += 1
        if receive:
            self.receive_errors += 1
        self.last_error = message
        self.last_error_at = datetime.now(timezone.utc)

    def record_connect_attempt(self) -> None:
        self.connect_attempts += 1

    def record_connect_failure(self) -> None:
        self.connect_failures += 1

    def record_disconnect(self) -> None:
        self.disconnects += 1

    def summary(self) -> str:
        return (
            f"tx={self.messages_sent}({self.bytes_sent}B) "
            f"rx={self.messages_received}({self.bytes_received}B) "
            f"send_errors={self.send_errors} receive_errors={self.receive_errors} "
            f"connect_attempts={self.connect_attempts} connect_failures={self.connect_failures} "
            f"disconnects={self.disconnects}"
        )
