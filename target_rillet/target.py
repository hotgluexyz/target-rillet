"""Rillet target class."""

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue
from typing import Type
from hotglue_singer_sdk.sinks import Sink
from target_rillet.sinks import (
    JournalsSink,
    FallbackSink,
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
        FallbackSink,
    ]

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        """Get sink for a stream."""
        stream_lower = stream_name.lower()
        for sink_class in self.SINK_TYPES:
            # Class-level str (e.g. JournalsSink.name); @property on class is not a str.
            sink_name = getattr(sink_class, "name", None)
            if isinstance(sink_name, str) and sink_name.lower() == stream_lower:
                return sink_class
        return FallbackSink


if __name__ == "__main__":
    TargetRillet.cli()
