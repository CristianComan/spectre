import random

from spectre.ew import EWDetectionSource

CFG = {
    "location_reference": {"latitude": 50.0, "longitude": 4.0, "altitude": 10.0},
    "detection_confidence": 0.9,
    "emitters": [
        {
            "name": "ALWAYS_ON",
            "classification": "RF Emitter - Test",
            "centre_frequency": 2400000000.0,
            "bandwidth": 20000000.0,
            "amplitude_mean": -50.0,
            "amplitude_jitter": 2.0,
            "on_probability": 1.0,
            "location_jitter_deg": 0.001,
        },
        {
            "name": "NEVER_ON",
            "classification": "RF Emitter - Test2",
            "centre_frequency": 5000000000.0,
            "bandwidth": 10000000.0,
            "amplitude_mean": -60.0,
            "amplitude_jitter": 1.0,
            "on_probability": 0.0,
            "location_jitter_deg": 0.001,
        },
    ],
}


def test_always_on_emitter_keeps_stable_object_id_across_samples():
    source = EWDetectionSource(CFG, rng=random.Random(0))
    first = source.sample()
    second = source.sample()

    assert len(first) == 1
    assert first[0].object_id == second[0].object_id
    assert first[0].classification == "RF Emitter - Test"


def test_never_on_emitter_is_never_sampled():
    source = EWDetectionSource(CFG, rng=random.Random(0))
    for _ in range(10):
        assert all(d.classification != "RF Emitter - Test2" for d in source.sample())


def test_frequency_band_matches_centre_and_bandwidth():
    source = EWDetectionSource(CFG, rng=random.Random(1))
    (detection,) = source.sample()

    assert detection.centre_frequency == 2400000000.0
    assert detection.start_frequency == 2400000000.0 - 10000000.0
    assert detection.stop_frequency == 2400000000.0 + 10000000.0


class _ScriptedRng:
    """Fake RNG whose on/off roll is scripted; other calls fall back to a seeded Random."""

    def __init__(self, on_off_script: list[bool]):
        self._script = list(on_off_script)
        self._fallback = random.Random(0)

    def random(self) -> float:
        # on_probability is 1.0 in CFG; only 0.0 is guaranteed "on"
        # (`<= 1.0`), and only >1.0 is guaranteed "off" regardless of it.
        return 0.0 if self._script.pop(0) else 2.0

    def uniform(self, a: float, b: float) -> float:
        return self._fallback.uniform(a, b)


def test_reappearing_emitter_gets_a_new_object_id():
    single_emitter_cfg = {**CFG, "emitters": [CFG["emitters"][0]]}
    source = EWDetectionSource(single_emitter_cfg, rng=_ScriptedRng([True, False, True]))

    (first,) = source.sample()
    assert source.sample() == []  # emitter off this cycle -> track dropped
    (second,) = source.sample()

    assert second.object_id != first.object_id
