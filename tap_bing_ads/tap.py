"""BingAds tap class."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta, timezone
from functools import cached_property

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.singerlib.catalog import Metadata, MetadataMapping
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
    streams.AssetDailyPerformanceStream,
    streams.PublisherUsageDailyPerformanceStream,
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

            # resolve primary keys from selected attribute columns
            stream.primary_keys = self._filter_selected_column_attribute_properties(
                # `Stream.primary_keys` set from from input catalog if provided - see
                # `Stream.apply_catalog` and `Tap.streams
                # otherwise construct from schema
                stream.primary_keys or stream.schema["properties"],
                stream.metadata,
            )

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
    @cached_property
    def catalog(self):
        catalog = super().catalog

        for tap_stream_id, entry in catalog.items():
            if not (stream := self.streams.get(tap_stream_id)):
                continue

            # primary key filtering happens at the stream level with respect to the
            # input catalog, so we need to reassign key properties here before passing
            # to setup mapper when we have stream aliases defined in config (otherwise,
            # this results in the error `singer_sdk.exceptions.StreamMapConfigError:
            # Invalid key properties for ...`)
            entry.key_properties = stream.primary_keys

        return catalog

    @override
    def discover_streams(self):
        return [stream_cls(tap=self) for stream_cls in STREAM_TYPES]

    @staticmethod
    def _filter_selected_column_attribute_properties(
        properties: t.Iterable[str],
        metadata: MetadataMapping,
    ) -> list[str]:
        return [
            p
            for p in properties
            if p in streams.REPORT_ATTRIBUTE_COLUMNS
            if (m := metadata[("properties", p)]).selected is not False
            or m.inclusion == Metadata.InclusionType.AUTOMATIC
        ]


if __name__ == "__main__":
    TapBingAds.cli()
