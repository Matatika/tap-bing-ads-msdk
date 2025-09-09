"""Stream type classes for tap-bing-ads."""

from __future__ import annotations

import csv
import fnmatch
import gzip
import tempfile
import time
from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers
from typing_extensions import override

from tap_bing_ads.client import BingAdsStream

BULK_DOWNLOAD_MAX_ATTEMPTS = 60


class _AccountInfoStream(BingAdsStream):
    name = "_account_info"
    selected = False

    schema = th.PropertiesList(
        th.Property("AccountLifeCycleStatus", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Name", th.StringType),
        th.Property("Number", th.StringType),
        th.Property("PauseReason", th.IntegerType),
    ).to_dict()

    http_method = "POST"
    url_base = "https://clientcenter.api.bingads.microsoft.com"
    path = "/CustomerManagement/v13/AccountsInfo/Query"
    records_jsonpath = "$.AccountsInfo[*]"

    @override
    def prepare_request_payload(self, context, next_page_token):
        return {"CustomerId": self.config["customer_id"]}

    @override
    def get_child_context(self, record, context):
        return {"account_id": record["Id"]}


class AccountStream(BingAdsStream):
    """Define accounts stream."""

    parent_stream_type = _AccountInfoStream
    name = "accounts"

    schema = th.PropertiesList(
        th.Property("AccountFinancialStatus", th.StringType),
        th.Property("AccountLifeCycleStatus", th.StringType),
        th.Property("AccountMode", th.StringType),
        th.Property("AutoTagType", th.StringType),
        th.Property("BackUpPaymentInstrumentId", th.StringType),
        th.Property("BillingThresholdAmount", th.StringType),
        th.Property("BillToCustomerId", th.StringType),
        th.Property(
            "BusinessAddress",
            th.ObjectType(
                th.Property("City", th.StringType),
                th.Property("CountryCode", th.StringType),
                th.Property("Id", th.StringType),
                th.Property("Line1", th.StringType),
                th.Property("Line2", th.StringType),
                th.Property("Line3", th.StringType),
                th.Property("Line4", th.StringType),
                th.Property("PostalCode", th.StringType),
                th.Property("StateOrProvince", th.StringType),
                th.Property("TimeStamp", th.StringType),
                th.Property("BusinessName", th.DateTimeType),
            ),
        ),
        th.Property("CurrencyCode", th.StringType),
        th.Property("ForwardCompatibilityMap", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Language", th.StringType),
        th.Property("LastModifiedByUserId", th.StringType),
        th.Property("LastModifiedTime", th.DateTimeType),
        th.Property(
            "LinkedAgencies",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Id", th.StringType),
                    th.Property("Name", th.StringType),
                )
            ),
        ),
        th.Property("Name", th.StringType),
        th.Property("Number", th.StringType),
        th.Property("ParentCustomerId", th.StringType),
        th.Property("PauseReason", th.IntegerType),
        th.Property("PaymentMethodId", th.StringType),
        th.Property("PaymentMethodType", th.StringType),
        th.Property("PrimaryUserId", th.StringType),
        th.Property("SalesHouseCustomerId", th.StringType),
        th.Property("SoldToPaymentInstrumentId", th.StringType),
        th.Property("TaxCertificate", th.StringType),
        th.Property(
            "TaxInformation",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Key", th.StringType),
                    th.Property("Value", th.StringType),
                ),
            ),
        ),
        th.Property("TimeStamp", th.StringType),
        th.Property("TimeZone", th.StringType),
    ).to_dict()

    http_method = "POST"
    url_base = "https://clientcenter.api.bingads.microsoft.com"
    path = "/CustomerManagement/v13/Account/Query"
    records_jsonpath = "$.Account"

    @override
    def prepare_request_payload(self, context, next_page_token):
        return {"AccountId": context["account_id"]}


class _BulkStream(BingAdsStream):
    parent_stream_type = _AccountInfoStream

    download_entity_name: str = ...
    entity_type_pattern: str = ...

    @property
    def http_headers(self):
        http_headers = super().http_headers
        http_headers["CustomerAccountId"] = self.context["account_id"]

        return http_headers

    @override
    def get_records(self, context):
        account_id = context["account_id"]
        bulk_file = (
            Path(tempfile.gettempdir())
            / f"{self.tap_name}_{self._tap.initialized_at}"
            / f"{account_id}.csv.gz"
        )

        if not bulk_file.exists():
            self._download_bulk_file(account_id, bulk_file)

        with gzip.open(bulk_file, "rt", encoding="utf-8-sig") as f:
            self.logger.info(
                "Processing file '%s' for account: %s",
                f.name,
                context["account_id"],
            )

            start = None

            for i, row in enumerate(csv.DictReader(f), start=2):
                if fnmatch.fnmatch(row["Type"], self.entity_type_pattern):
                    start = i
                    yield row
                    continue

                if start:
                    return

    @override
    def post_process(self, row, context=None):
        for k, v in row.copy().items():
            if k not in self.schema["properties"]:
                row.pop(k)
                continue

            if v == "":
                row[k] = None

        return row

    def _download_bulk_file(self, account_id: str, bulk_file: Path):
        response = self.requests_session.post(
            "https://bulk.api.bingads.microsoft.com/Bulk/v13/Campaigns/DownloadByAccountIds",
            json={
                "AccountIds": [account_id],
                "CompressionType": "GZip",
                "DownloadEntities": [
                    s.download_entity_name
                    for s in self._tap.streams.values()
                    if isinstance(s, _BulkStream)
                ],
                "FormatVersion": "6.0",
            },
            headers=self.http_headers,
            auth=self.authenticator,
        )
        response.raise_for_status()

        request_id = response.json()["DownloadRequestId"]
        attempts = 0

        while True:
            response = self.requests_session.post(
                "https://bulk.api.bingads.microsoft.com/Bulk/v13/BulkDownloadStatus/Query",
                json={"RequestId": request_id},
                headers=self.http_headers,
                auth=self.authenticator,
            )
            response.raise_for_status()

            status = response.json()
            request_status = status["RequestStatus"]
            attempts += 1

            self.logger.info(
                "Download %d%% complete (%s)",
                status["PercentComplete"],
                request_status,
            )

            if request_status == "InProgress":
                if attempts >= BULK_DOWNLOAD_MAX_ATTEMPTS:
                    msg = (
                        f"Download incomplete ({request_status}) after checking "
                        f"{attempts} time(s)"
                    )
                    raise RuntimeError(msg)

                time.sleep(1)
                continue

            if request_status == "Completed":
                break

            msg = "Download failed ({}): {}".format(request_status, status["Errors"])
            raise RuntimeError(msg)

        bulk_file.parent.mkdir(exist_ok=True)

        with (
            self.requests_session.get(status["ResultFileUrl"], stream=True) as r,
            bulk_file.open("wb") as f,
        ):
            r.raise_for_status()

            for chunk in r.iter_content(chunk_size=8192):  # 8 KB chunks
                if chunk:  # skip keep-alive chunks
                    f.write(chunk)


class CampaignStream(_BulkStream):
    """Define campaigns stream."""

    name = "campaigns"

    schema = th.PropertiesList(
        th.Property("App Id", th.StringType),
        th.Property("App Store", th.StringType),
        th.Property("Auto Generated Image Assets Opt Out", th.StringType),
        th.Property("Auto Generated Text Assets Opt Out", th.StringType),
        th.Property("Automated Call To Action Opt Out", th.StringType),
        th.Property("Ad Schedule Use Searcher Time Zone", th.StringType),
        th.Property("Bid Adjustment", th.StringType),
        th.Property("Bid Strategy Commission", th.StringType),
        th.Property("Bid Strategy Id", th.StringType),
        th.Property("Bid Strategy ManualCpc", th.StringType),
        th.Property("Bid Strategy MaxCpc", th.StringType),
        th.Property("Bid Strategy Name", th.StringType),
        th.Property("Bid Strategy PercentCpc", th.StringType),
        th.Property("Bid Strategy TargetAdPosition", th.StringType),
        th.Property("Bid Strategy TargetCostPerSale", th.StringType),
        th.Property("Bid Strategy TargetCpa", th.StringType),
        th.Property("Bid Strategy TargetImpressionShare", th.StringType),
        th.Property("Bid Strategy TargetRoas", th.StringType),
        th.Property("Bid Strategy Type", th.StringType),
        th.Property("Budget", th.StringType),
        th.Property("Budget Id", th.StringType),
        th.Property("Budget Name", th.StringType),
        th.Property("Budget Type", th.StringType),
        th.Property("Campaign", th.StringType),
        th.Property("Campaign Type", th.StringType),
        th.Property("Client Id", th.StringType),
        th.Property("Country Code", th.StringType),
        th.Property("Custom Parameter", th.StringType),
        th.Property("Destination Channel", th.StringType),
        th.Property("Domain Language", th.StringType),
        th.Property("Disclaimer Ads Enabled", th.StringType),
        th.Property("Dynamic Description Enabled", th.StringType),
        th.Property("Experiment Id", th.StringType),
        th.Property("Feed Label", th.StringType),
        th.Property("Final Url Suffix", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Is Multi Channel Campaign", th.StringType),
        th.Property("Keyword Relevance", th.StringType),
        th.Property("Landing Page Relevance", th.StringType),
        th.Property("Landing Page User Experience", th.StringType),
        th.Property("Language", th.StringType),
        th.Property("Local Inventory Ads Enabled", th.StringType),
        th.Property("Modified Time", th.StringType),
        th.Property("Page Feed Ids", th.StringType),
        th.Property("Parent Id", th.StringType),
        th.Property("Priority", th.StringType),
        th.Property("Quality Score", th.StringType),
        th.Property("RSA Auto Generated Assets Enabled", th.StringType),
        th.Property("Sales Country Code", th.StringType),
        th.Property("Shoppable Ads Enabled", th.StringType),
        th.Property("Source", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("Store Id", th.StringType),
        th.Property("Sub Type", th.StringType),
        th.Property("Target Setting", th.StringType),
        th.Property("Tracking Template", th.StringType),
        th.Property("Vanity Pharma Display Url Mode", th.StringType),
        th.Property("Vanity Pharma Website Description", th.StringType),
        th.Property("URL Expansion Opt Out", th.StringType),
        th.Property("Website", th.StringType),
    ).to_dict()

    download_entity_name = "Campaigns"
    entity_type_pattern = "Campaign"


class AdGroupStream(_BulkStream):
    """Define ad groups stream."""

    name = "ad_groups"

    schema = th.PropertiesList(
        th.Property("Ad Group", th.StringType),
        th.Property("Ad Group Type", th.StringType),
        th.Property("Ad Rotation", th.StringType),
        th.Property("Ad Schedule Use Searcher Time Zone", th.StringType),
        th.Property("Bid Adjustment", th.StringType),
        th.Property("Bid Boost Value", th.StringType),
        th.Property("Bid Option", th.StringType),
        th.Property("Bid Strategy Type", th.StringType),
        th.Property("Campaign", th.StringType),
        th.Property("Client Id", th.StringType),
        th.Property("Cpc Bid", th.StringType),
        th.Property("Custom Parameter", th.StringType),
        th.Property("End Date", th.StringType),
        th.Property("Final Url Suffix", th.StringType),
        th.Property("Frequency Cap Settings", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Inherited Bid Strategy Type", th.StringType),
        th.Property("Keyword Relevance", th.StringType),
        th.Property("Landing Page Relevance", th.StringType),
        th.Property("Landing Page User Experience", th.StringType),
        th.Property("Language", th.StringType),
        th.Property("Maximum Bid", th.StringType),
        th.Property("Modified Time", th.StringType),
        th.Property("Network Distribution", th.StringType),
        th.Property("Parent Id", th.StringType),
        th.Property("Privacy Status", th.StringType),
        th.Property("Quality Score", th.StringType),
        th.Property("Start Date", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("Target Setting", th.StringType),
        th.Property("Tracking Template", th.StringType),
        th.Property("Use Optimized Targeting", th.StringType),
        th.Property("Use Predictive Targeting", th.StringType),
    ).to_dict()

    download_entity_name = "AdGroups"
    entity_type_pattern = "Ad Group"


class AdStream(_BulkStream):
    """Define ads stream."""

    name = "ads"
    schema = th.PropertiesList(
        th.Property("Ad Format Preference", th.StringType),
        th.Property("Ad Group", th.StringType),
        th.Property("App Id", th.StringType),
        th.Property("App Platform", th.StringType),
        th.Property("Business Name", th.StringType),
        th.Property("Call To Action", th.StringType),
        th.Property("Call To Action Langauge", th.StringType),
        th.Property("Campaign", th.StringType),
        th.Property("Client Id", th.StringType),
        th.Property("Custom Parameter", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Descriptions", th.StringType),
        th.Property("Device Preference", th.StringType),
        th.Property("Display Url", th.StringType),
        th.Property("Domain", th.StringType),
        th.Property("Editorial Appeal Status", th.StringType),
        th.Property("Editorial Location", th.StringType),
        th.Property("Editorial Reason Code", th.StringType),
        th.Property("Editorial Status", th.StringType),
        th.Property("Editorial Term", th.StringType),
        th.Property("Final Url", th.StringType),
        th.Property("Final Url Suffix", th.StringType),
        th.Property("Headline", th.StringType),
        th.Property("Headlines", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Images", th.StringType),
        th.Property("Impression Tracking Urls", th.StringType),
        th.Property("Landscape Image Media Id", th.StringType),
        th.Property("Landscape Logo Media Id", th.StringType),
        th.Property("Long Headline", th.StringType),
        th.Property("Long Headline String", th.StringType),
        th.Property("Long Headlines", th.StringType),
        th.Property("Mobile Final Url", th.StringType),
        th.Property("Modified Time", th.StringType),
        th.Property("Parent Id", th.StringType),
        th.Property("Path 1", th.StringType),
        th.Property("Path 2", th.StringType),
        th.Property("Publisher Countries", th.StringType),
        th.Property("Square Image Media Id", th.StringType),
        th.Property("Square Logo Media Id", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("Text", th.StringType),
        th.Property("Text Part 2", th.StringType),
        th.Property("Title", th.StringType),
        th.Property("Title Part 1", th.StringType),
        th.Property("Title Part 2", th.StringType),
        th.Property("Title Part 3", th.StringType),
        th.Property("Tracking Template", th.StringType),
    ).to_dict()

    download_entity_name = "Ads"
    entity_type_pattern = "* Ad"


class KeywordStream(_BulkStream):
    """Define keyword stream."""

    name = "keywords"
    schema = th.PropertiesList(
        th.Property("Ad Group", th.StringType),
        th.Property("Bid", th.StringType),
        th.Property("Bid Strategy Type", th.StringType),
        th.Property("Campaign", th.StringType),
        th.Property("Client Id", th.StringType),
        th.Property("Custom Parameter", th.StringType),
        th.Property("Destination Url", th.StringType),
        th.Property("Editorial Appeal Status", th.StringType),
        th.Property("Editorial Location", th.StringType),
        th.Property("Editorial Reason Code", th.StringType),
        th.Property("Editorial Status", th.StringType),
        th.Property("Editorial Term", th.StringType),
        th.Property("Final Url", th.StringType),
        th.Property("Final Url Suffix", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Inherited Bid Strategy Type", th.StringType),
        th.Property("Keyword", th.StringType),
        th.Property("Keyword Relevance", th.StringType),
        th.Property("Landing Page Relevance", th.StringType),
        th.Property("Landing Page User Experience", th.StringType),
        th.Property("Match Type", th.StringType),
        th.Property("Mobile Final Url", th.StringType),
        th.Property("Modified Time", th.StringType),
        th.Property("Param1", th.StringType),
        th.Property("Param2", th.StringType),
        th.Property("Param3", th.StringType),
        th.Property("Parent Id", th.StringType),
        th.Property("Publisher Countries", th.StringType),
        th.Property("Quality Score", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("Tracking Template", th.StringType),
    ).to_dict()

    download_entity_name = "Keywords"
    entity_type_pattern = "Keyword"
