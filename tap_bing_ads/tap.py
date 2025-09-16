"""BingAds tap class."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import cached_property

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.singerlib.catalog import Metadata
from typing_extensions import override

from tap_bing_ads import streams

STREAM_TYPES = [
    streams._AccountInfoStream,  # noqa: SLF001
    streams._AccountContextStream,  # noqa: SLF001
    streams.AccountStream,
    streams.CampaignStream,
    streams.AdGroupStream,
    streams.AdStream,
    streams.KeywordStream,
    streams.AdGroupDailyPerformanceStream,
    streams.AdDailyPerformanceStream,
    streams.KeywordDailyPerformanceStream,
]


class TapBingAds(Tap):
    """BingAds tap class."""

    name = "tap-bing-ads"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            title="Client ID",
            description="App OAuth 2.0 client secret",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            title="Client secret",
            description="App OAuth 2.0 client secret",
            secret=True,
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            title="Refresh secret",
            description="App OAuth 2.0 refresh token",
            secret=True,
        ),
        th.Property(
            "developer_token",
            th.StringType,
            required=True,
            title="Developer token",
            description="Microsoft Advertising devleper token",
            secret=True,
        ),
        th.Property(
            "customer_id",
            th.IntegerType,
            required=True,
            title="Customer ID",
            description="ID of customer to extract data for",
        ),
        th.Property(
            "account_ids",
            th.ArrayType(th.IntegerType),
            title="Account IDs",
            description="Filter accounts to extract data for by ID",
            default=[],
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            title="Start date",
            description="The earliest record date to sync",
            default=(
                (datetime.now(tz=timezone.utc) - timedelta(days=365)).date().isoformat()
            ),
        ),
    ).to_dict()

    @override
    @cached_property
    def catalog(self):
        catalog = super().catalog

        for stream in self.streams.values():
            if not isinstance(stream, streams._DailyPerformanceReportStream):  # noqa: SLF001
                continue

            if not (entry := catalog.get(stream.name)):
                continue

            if entry.key_properties:
                continue

            # resolve key properties from selected attribute columns
            entry.key_properties = []

            for p in entry.schema.properties:
                if p not in streams.REPORT_ATTRIBUTE_COLUMNS:
                    continue

                metadata = entry.metadata[("properties", p)]

                if (
                    metadata.inclusion == Metadata.InclusionType.AUTOMATIC
                    or metadata.selected
                ):
                    entry.key_properties.append(p)

            self.logger.info(
                (
                    "Automatically resolved key properties for stream '%s' from "
                    "selected attribute columns: %s"
                ),
                stream.name,
                entry.key_properties,
            )

        return catalog

    @override
    def discover_streams(self):
        return [stream_cls(tap=self) for stream_cls in STREAM_TYPES]


if __name__ == "__main__":
    TapBingAds.cli()
