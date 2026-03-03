"""Tests standard target features using the built-in SDK tests library."""

from typing import Dict, Any

from hotglue_singer_sdk.testing import get_standard_target_tests

from target_rillet.target import TargetRillet

SAMPLE_CONFIG: Dict[str, Any] = {
    # TODO: Initialize minimal target config
}


def test_standard_target_tests():
    """Run standard target tests from the SDK."""
    tests = get_standard_target_tests(
        TargetRillet,
        config=SAMPLE_CONFIG,
    )
    for test in tests:
        test()
