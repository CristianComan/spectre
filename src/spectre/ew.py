from __future__ import annotations

import random
from dataclasses import dataclass

from .ids import new_ulid


@dataclass(frozen=True)
class EmitterProfile:
    name: str
    classification: str
    centre_frequency: float
    bandwidth: float
    amplitude_mean: float
    amplitude_jitter: float
    on_probability: float
    location_jitter_deg: float
    pulse_duration: float | None = None


@dataclass(frozen=True)
class Detection:
    object_id: str
    classification: str
    detection_confidence: float
    amplitude: float
    start_frequency: float
    centre_frequency: float
    stop_frequency: float
    pulse_duration: float | None
    latitude: float
    longitude: float
    altitude: float


def _parse_profile(e: dict) -> EmitterProfile:
    return EmitterProfile(
        name=e["name"],
        classification=e["classification"],
        centre_frequency=float(e["centre_frequency"]),
        bandwidth=float(e["bandwidth"]),
        amplitude_mean=float(e["amplitude_mean"]),
        amplitude_jitter=float(e.get("amplitude_jitter", 0.0)),
        on_probability=float(e["on_probability"]),
        location_jitter_deg=float(e.get("location_jitter_deg", 0.0)),
        pulse_duration=(float(e["pulse_duration"]) if e.get("pulse_duration") is not None else None),
    )


class EWDetectionSource:
    """Simulates a small population of RF/EW emitters for protocol-level testing.

    Each emitter independently rolls on/off every sampling cycle. A newly
    appearing emitter is a new detected object (fresh ULID object_id); while it
    stays active across consecutive cycles its object_id is held stable, so it
    reads as one continuous ESM track rather than a new detection each time.
    """

    def __init__(self, cfg: dict, rng: random.Random | None = None):
        self._rng = rng or random.Random()
        ref = cfg["location_reference"]
        self._ref_lat = float(ref["latitude"])
        self._ref_lon = float(ref["longitude"])
        self._ref_alt = float(ref.get("altitude", 0.0))
        self._base_confidence = float(cfg.get("detection_confidence", 0.9))
        self._profiles = [_parse_profile(e) for e in cfg["emitters"]]
        self._active_ids: dict[str, str] = {}

    def sample(self) -> list[Detection]:
        detections = []
        for profile in self._profiles:
            if self._rng.random() > profile.on_probability:
                self._active_ids.pop(profile.name, None)
                continue

            object_id = self._active_ids.get(profile.name)
            if object_id is None:
                object_id = new_ulid()
                self._active_ids[profile.name] = object_id

            amplitude = profile.amplitude_mean + self._rng.uniform(
                -profile.amplitude_jitter, profile.amplitude_jitter
            )
            half_bw = profile.bandwidth / 2.0
            lat = self._ref_lat + self._rng.uniform(-1.0, 1.0) * profile.location_jitter_deg
            lon = self._ref_lon + self._rng.uniform(-1.0, 1.0) * profile.location_jitter_deg
            confidence = self._base_confidence + self._rng.uniform(-0.05, 0.05)

            detections.append(
                Detection(
                    object_id=object_id,
                    classification=profile.classification,
                    detection_confidence=min(1.0, max(0.0, confidence)),
                    amplitude=amplitude,
                    start_frequency=profile.centre_frequency - half_bw,
                    centre_frequency=profile.centre_frequency,
                    stop_frequency=profile.centre_frequency + half_bw,
                    pulse_duration=profile.pulse_duration,
                    latitude=lat,
                    longitude=lon,
                    altitude=self._ref_alt,
                )
            )
        return detections
