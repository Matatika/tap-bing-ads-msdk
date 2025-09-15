"""BingAds tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
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
            title="Client ID",
            description="App OAuth 2.0 client secret",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            title="Client secret",
            description="App OAuth 2.0 client secret",
            secret=True,
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            title="Refresh secret",
            description="App OAuth 2.0 refresh token",
            secret=True,
        ),
        th.Property(
            "developer_token",
            th.StringType,
            title="Developer token",
            description="Microsoft Advertising devleper token",
            secret=True,
        ),
        th.Property(
            "customer_id",
            th.StringType,
            title="Customer ID",
            description="ID of customer to extract data for",
        ),
        th.Property(
            "account_ids",
            th.ArrayType(th.IntegerType),
            title="Account IDs",
            description=(
                "IDs of accounts to extract data for (defaults to all if not specified)"
            ),
            default=[],
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
    ).to_dict()

    @override
    def discover_streams(self):
        return [stream_cls(tap=self) for stream_cls in STREAM_TYPES]


if __name__ == "__main__":
    TapBingAds.cli()
