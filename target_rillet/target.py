"""Rillet target class."""

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue
from target_rillet.sinks import (
    JournalsSink,
)


class TargetRillet(TargetHotglue):
    """Singer target for Rillet."""

    name = "target-rillet"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            description="Your Rillet API key for authentication",
        ),
        th.Property(
            "sandbox",
            th.BooleanType,
            description="Use the Rillet sandbox environment",
            default=False,
        ),
    ).to_dict()

    SINK_TYPES = [
        JournalsSink,
    ]


if __name__ == "__main__":
    TargetRillet.cli()
