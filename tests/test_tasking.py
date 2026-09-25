import asyncio

from spectre.client import (
    SapientEdgeClient,
    TASK_REASON_CONCURRENT_TASK_LIMIT,
    TASK_REASON_RESOURCE_UNAVAILABLE,
    TASK_REASON_UNSUPPORTED_COMMAND,
    TASK_REASON_UNSUPPORTED_MODE,
)
from spectre.proto import Task, TaskAck

CFG = {
    "fusion_node": {"host": "127.0.0.1", "port": 0},
    "node": {
        "node_id": "11111111-1111-1111-1111-111111111111",
        "registration_file": "config/registration.json",
    },
    "status": {"interval_s": 5.0, "mode": "MONITOR"},
}

FUSION_NODE_ID = "550e8400-e29b-41d4-a716-446655440000"


def _make_client():
    client = SapientEdgeClient(CFG)
    sent: list = []

    async def fake_send(msg):
        sent.append(msg)

    client.send = fake_send
    return client, sent


def _task(control, task_id: str) -> Task:
    task = Task()
    task.task_id = task_id
    task.control = control
    return task


def _mode_change_task(mode: str, task_id: str) -> Task:
    task = _task(Task.CONTROL_START, task_id)
    task.command.mode_change = mode
    return task


def test_mode_change_accepted_updates_mode_and_active_task():
    client, sent = _make_client()

    asyncio.run(client.handle_task(_mode_change_task("MONITOR", "task-1"), FUSION_NODE_ID))

    assert len(sent) == 2
    ack_msg, status_msg = sent
    ack = ack_msg.task_ack
    assert ack.task_status == TaskAck.TASK_STATUS_ACCEPTED
    assert ack_msg.destination_id == FUSION_NODE_ID
    assert client.current_mode == "MONITOR"
    assert client.active_task_id == "task-1"

    # An immediate out-of-cycle StatusReport follows the accepted mode change,
    # surfacing the mode-history transition so it's visible on the Fusion
    # Node / C2 side, not just in local logs.
    assert status_msg.WhichOneof("content") == "status_report"
    assert len(status_msg.status_report.status) == 1
    assert "MONITOR" in status_msg.status_report.status[0].status_value
    assert client.mode_history.last is not None
    assert client.mode_history.last.to_mode == "MONITOR"
    assert client.mode_history.last.task_id == "task-1"


def test_mode_change_rejected_for_unregistered_mode():
    client, sent = _make_client()

    asyncio.run(client.handle_task(_mode_change_task("ATTACK", "task-1"), FUSION_NODE_ID))

    ack = sent[0].task_ack
    assert ack.task_status == TaskAck.TASK_STATUS_REJECTED
    assert TASK_REASON_UNSUPPORTED_MODE in ack.reason
    assert client.active_task_id is None


def test_second_concurrent_mode_change_is_rejected():
    client, sent = _make_client()

    asyncio.run(client.handle_task(_mode_change_task("MONITOR", "task-1"), FUSION_NODE_ID))
    asyncio.run(client.handle_task(_mode_change_task("MONITOR", "task-2"), FUSION_NODE_ID))

    ack = sent[-1].task_ack
    assert ack.task_status == TaskAck.TASK_STATUS_REJECTED
    assert TASK_REASON_CONCURRENT_TASK_LIMIT in ack.reason
    assert client.active_task_id == "task-1"


def test_unsupported_command_is_rejected():
    client, sent = _make_client()
    task = _task(Task.CONTROL_START, "task-1")
    task.command.detection_threshold = Task.DISCRETE_THRESHOLD_LOW

    asyncio.run(client.handle_task(task, FUSION_NODE_ID))

    ack = sent[0].task_ack
    assert ack.task_status == TaskAck.TASK_STATUS_REJECTED
    assert TASK_REASON_UNSUPPORTED_COMMAND in ack.reason


def test_stop_clears_active_task_and_reverts_mode():
    client, sent = _make_client()
    asyncio.run(client.handle_task(_mode_change_task("MONITOR", "task-1"), FUSION_NODE_ID))

    asyncio.run(client.handle_task(_task(Task.CONTROL_STOP, "task-1"), FUSION_NODE_ID))

    # sent so far: [mode-change ack, mode-change status, stop ack, stop status]
    stop_ack_msg = sent[2]
    assert stop_ack_msg.task_ack.task_status == TaskAck.TASK_STATUS_ACCEPTED
    assert client.active_task_id is None
    assert client.current_mode == client.default_mode
    assert client.active_task_regions == []


def test_stop_on_unknown_task_is_rejected():
    client, sent = _make_client()

    asyncio.run(client.handle_task(_task(Task.CONTROL_STOP, "no-such-task"), FUSION_NODE_ID))

    ack = sent[-1].task_ack
    assert ack.task_status == TaskAck.TASK_STATUS_REJECTED
    assert TASK_REASON_RESOURCE_UNAVAILABLE in ack.reason


def test_request_status_acks_then_sends_immediate_status_report():
    client, sent = _make_client()
    task = _task(Task.CONTROL_START, "task-1")
    task.command.request = "status"

    asyncio.run(client.handle_task(task, FUSION_NODE_ID))

    assert len(sent) == 2
    ack_msg, status_msg = sent
    assert ack_msg.task_ack.task_status == TaskAck.TASK_STATUS_ACCEPTED
    assert status_msg.WhichOneof("content") == "status_report"
    assert status_msg.status_report.mode == client.current_mode


def test_mode_change_with_region_stores_it_ack_only():
    client, sent = _make_client()
    task = _mode_change_task("MONITOR", "task-1")
    region = task.region.add()
    region.type = Task.REGION_TYPE_AREA_OF_INTEREST
    region.region_id = "01M3C8V59DKSPYQTQJVF1DDS33"
    region.region_name = "North Sector"

    asyncio.run(client.handle_task(task, FUSION_NODE_ID))

    ack = sent[0].task_ack
    assert ack.task_status == TaskAck.TASK_STATUS_ACCEPTED
    assert len(client.active_task_regions) == 1
    assert client.active_task_regions[0]["region_name"] == "North Sector"

    asyncio.run(client.handle_task(_task(Task.CONTROL_STOP, "task-1"), FUSION_NODE_ID))
    assert client.active_task_regions == []


def test_request_registration_acks_then_resends_registration():
    client, sent = _make_client()
    task = _task(Task.CONTROL_START, "task-1")
    task.command.request = "registration"

    asyncio.run(client.handle_task(task, FUSION_NODE_ID))

    assert len(sent) == 2
    ack_msg, registration_msg = sent
    assert ack_msg.task_ack.task_status == TaskAck.TASK_STATUS_ACCEPTED
    assert registration_msg.WhichOneof("content") == "registration"
