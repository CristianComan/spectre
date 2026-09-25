from __future__ import annotations

try:
    from sapient_msg.bsi_flex_335_v2_0.sapient_message_pb2 import SapientMessage
    from sapient_msg.bsi_flex_335_v2_0.registration_pb2 import Registration
    from sapient_msg.bsi_flex_335_v2_0.status_report_pb2 import StatusReport
    from sapient_msg.bsi_flex_335_v2_0.detection_report_pb2 import DetectionReport
    from sapient_msg.bsi_flex_335_v2_0.task_pb2 import Task
    from sapient_msg.bsi_flex_335_v2_0.task_ack_pb2 import TaskAck
except ModuleNotFoundError as exc:
    raise RuntimeError(
        "Generated SAPIENT protobuf bindings are missing. "
        "Run ./scripts/fetch_protos.sh and ./scripts/compile_protos.sh."
    ) from exc

__all__ = ["SapientMessage", "Registration", "StatusReport", "DetectionReport", "Task", "TaskAck"]
