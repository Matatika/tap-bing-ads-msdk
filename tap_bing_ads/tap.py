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
    def streams(self):
        streams_ = super().streams

        for stream in streams_.values():
            if not isinstance(stream, streams._DailyPerformanceReportStream):  # noqa: SLF001
                continue

            if stream.primary_keys:
                continue

            # resolve primary keys from selected attribute columns
            stream.primary_keys = [
                p
                for p in stream.schema["properties"]
                if p in streams.REPORT_ATTRIBUTE_COLUMNS
                if (m := stream.metadata[("properties", p)]).selected is not False
                or m.inclusion == Metadata.InclusionType.AUTOMATIC
            ]

            self.logger.info(
                (
                    "Automatically resolved primary keys for stream '%s' from selected "
                    "attribute columns: %s"
                ),
                stream.name,
                stream.primary_keys,
            )

        return streams_

    @override
    def discover_streams(self):
        return [stream_cls(tap=self) for stream_cls in STREAM_TYPES]


if __name__ == "__main__":
    TapBingAds.cli()
