from spectre.netmon import NetworkStats


def test_starts_at_zero():
    stats = NetworkStats()
    assert stats.messages_sent == 0
    assert stats.messages_received == 0
    assert stats.send_errors == 0
    assert stats.receive_errors == 0
    assert stats.last_error is None


def test_record_sent_accumulates_counts_bytes_and_per_type():
    stats = NetworkStats()
    stats.record_sent("status_report", 128)
    stats.record_sent("status_report", 130)
    stats.record_sent("task_ack", 40)

    assert stats.messages_sent == 3
    assert stats.bytes_sent == 298
    assert stats.sent_by_type == {"status_report": 2, "task_ack": 1}


def test_record_received_accumulates_counts_bytes_and_per_type():
    stats = NetworkStats()
    stats.record_received("registration_ack", 94)
    stats.record_received("task", 200)

    assert stats.messages_received == 2
    assert stats.bytes_received == 294
    assert stats.received_by_type == {"registration_ack": 1, "task": 1}


def test_record_error_tracks_send_and_receive_independently():
    stats = NetworkStats()
    stats.record_error("boom-send", send=True)
    stats.record_error("boom-recv", receive=True)

    assert stats.send_errors == 1
    assert stats.receive_errors == 1
    assert stats.last_error == "boom-recv"
    assert stats.last_error_at is not None


def test_connect_and_disconnect_counters():
    stats = NetworkStats()
    stats.record_connect_attempt()
    stats.record_connect_attempt()
    stats.record_connect_failure()
    stats.record_disconnect()

    assert stats.connect_attempts == 2
    assert stats.connect_failures == 1
    assert stats.disconnects == 1


def test_summary_contains_key_counters():
    stats = NetworkStats()
    stats.record_sent("status_report", 128)
    stats.record_received("task", 50)
    stats.record_error("boom", send=True)

    summary = stats.summary()
    assert "tx=1" in summary
    assert "rx=1" in summary
    assert "send_errors=1" in summary
    assert "receive_errors=0" in summary
