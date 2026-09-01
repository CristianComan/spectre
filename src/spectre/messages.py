from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
import json

from google.protobuf.json_format import ParseDict
from google.protobuf.timestamp_pb2 import Timestamp

from .ids import new_ulid
from .proto import SapientMessage, Registration, StatusReport, DetectionReport

if TYPE_CHECKING:
    from .ew import Detection


def _now_timestamp() -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(datetime.now(timezone.utc))
    return ts


def _wrap(node_id: str, field_name: str, body) -> SapientMessage:
    msg = SapientMessage()
    msg.timestamp.CopyFrom(_now_timestamp())
    msg.node_id = node_id
    getattr(msg, field_name).CopyFrom(body)
    return msg


def build_registration(node_id: str, registration_file: str | Path) -> SapientMessage:
    body = Registration()
    data = json.loads(Path(registration_file).read_text(encoding="utf-8"))
    ParseDict(data, body, ignore_unknown_fields=False)
    return _wrap(node_id, "registration", body)


def _set_wgs84_location(location_msg, latitude: float, longitude: float, altitude: float) -> None:
    """Populate the BSI Flex 335 v2 Location message using WGS84 lat/lon degrees/metres."""
    # In the v2 schema: x = longitude, y = latitude, z = altitude.
    location_msg.x = longitude
    location_msg.y = latitude
    location_msg.z = altitude
    location_msg.coordinate_system = 1  # LOCATION_COORDINATE_SYSTEM_LAT_LNG_DEG_M
    location_msg.datum = 1              # LOCATION_DATUM_WGS84_E


def build_status(node_id: str, cfg: dict) -> SapientMessage:
    body = StatusReport()
    body.report_id = new_ulid()
    body.system = StatusReport.SYSTEM_OK
    body.info = StatusReport.INFO_NEW
    body.mode = cfg["status"]["mode"]

    loc = cfg["status"].get("node_location")
    if loc:
        _set_wgs84_location(
            body.node_location,
            float(loc["latitude"]),
            float(loc["longitude"]),
            float(loc.get("altitude", 0.0)),
        )

    return _wrap(node_id, "status_report", body)


def build_ew_detection(node_id: str, detection: "Detection") -> SapientMessage:
    """Build a DetectionReport for one EW/RF emitter sample (see ew.EWDetectionSource)."""
    body = DetectionReport()
    body.report_id = new_ulid()
    body.object_id = detection.object_id
    body.detection_confidence = detection.detection_confidence

    _set_wgs84_location(body.location, detection.latitude, detection.longitude, detection.altitude)

    classification = body.classification.add()
    classification.type = detection.classification
    classification.confidence = detection.detection_confidence

    sig = body.signal.add()
    sig.amplitude = detection.amplitude
    sig.start_frequency = detection.start_frequency
    sig.centre_frequency = detection.centre_frequency
    sig.stop_frequency = detection.stop_frequency
    if detection.pulse_duration is not None:
        sig.pulse_duration = detection.pulse_duration

    return _wrap(node_id, "detection_report", body)
