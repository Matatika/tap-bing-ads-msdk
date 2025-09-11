"""Stream type classes for tap-bing-ads."""

from __future__ import annotations

import csv
import fnmatch
import gzip
import io
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from functools import cached_property
from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers
from typing_extensions import override

from tap_bing_ads.client import BingAdsStream

BULK_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS = 60
REPORT_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS = 120


class _AccountInfoStream(BingAdsStream):
    name = "_account_info"
    selected = False
    primary_keys = ("Id",)

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
    primary_keys = ("Id",)

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
    primary_keys = ("Id",)

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
            self.logger.info("Processing file '%s' for account: %s", f.name, account_id)

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
        self.validate_response(response)

        request_id = response.json()["DownloadRequestId"]
        attempts = 0

        while True:
            response = self.requests_session.post(
                "https://bulk.api.bingads.microsoft.com/Bulk/v13/BulkDownloadStatus/Query",
                json={"RequestId": request_id},
                headers=self.http_headers,
                auth=self.authenticator,
            )
            self.validate_response(response)

            download_request_status = response.json()
            status = download_request_status["RequestStatus"]
            attempts += 1

            self.logger.info(
                "[%*d/%d] Bulk download request status: %s (%d%% complete)",
                len(str(BULK_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS)),
                attempts,
                BULK_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS,
                status,
                download_request_status["PercentComplete"],
            )

            if status == "InProgress":
                if attempts >= BULK_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS:
                    msg = (
                        f"Bulk download request incomplete ({status}) after checking "
                        f"{attempts} times"
                    )
                    raise RuntimeError(msg)

                time.sleep(1)
                continue

            if status == "Completed":
                break

            msg = "Bulk download request failed ({}): {}".format(
                status,
                download_request_status["Errors"],
            )
            raise RuntimeError(msg)

        download_url = download_request_status["ResultFileUrl"]
        bulk_file.parent.mkdir(exist_ok=True)

        with (
            self.requests_session.get(download_url, stream=True) as r,
            bulk_file.open("wb") as f,
        ):
            self.validate_response(r)

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


class _DailyPerformanceReportStream(BingAdsStream):
    parent_stream_type = _AccountInfoStream
    primary_keys = ("TimePeriod",)
    replication_key = "TimePeriod"
    is_timestamp_replication_key = True

    report_request_name: str = ...
    column_restrictions: tuple[tuple[set[str], set[str]], ...] = ()

    @override
    def get_records(self, context):
        account_id = context["account_id"]
        report_file = (
            Path(tempfile.gettempdir())
            / f"{self.tap_name}_{self._tap.initialized_at}"
            / f"{account_id}.zip"
        )

        start = self.get_starting_timestamp(context)
        now = datetime.now(tz=timezone.utc)

        response = self.requests_session.post(
            "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Submit",
            json={
                "ReportRequest": {
                    "Aggregation": "Daily",
                    "Columns": list(self.columns),
                    "ExcludeReportFooter": True,
                    "ExcludeReportHeader": True,
                    "FormatVersion": "2.0",
                    "Scope": {
                        "AccountIds": [account_id],
                    },
                    "Time": {
                        "CustomDateRangeEnd": {
                            "Day": now.day,
                            "Month": now.month,
                            "Year": now.year,
                        },
                        "CustomDateRangeStart": {
                            "Day": start.day,
                            "Month": start.month,
                            "Year": start.year,
                        },
                        "ReportTimeZone": "GreenwichMeanTimeDublinEdinburghLisbonLondon",  # noqa: E501
                    },
                    "Type": self.report_request_name,
                }
            },
            headers=self.http_headers,
            auth=self.authenticator,
        )
        self.validate_response(response)

        request_id = response.json()["ReportRequestId"]
        attempts = 0

        while True:
            response = self.requests_session.post(
                "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Poll",
                json={"ReportRequestId": request_id},
                headers=self.http_headers,
                auth=self.authenticator,
            )
            self.validate_response(response)

            report_request_status = response.json()["ReportRequestStatus"]
            status = report_request_status["Status"]
            attempts += 1

            self.logger.info(
                "[%*d/%d] %s status: %s",
                len(str(REPORT_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS)),
                attempts,
                REPORT_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS,
                self.report_request_name,
                status,
            )

            if status == "Pending":
                if attempts >= REPORT_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS:
                    msg = (
                        f"{self.report_request_name} incomplete ({status}) after "
                        f"checking {attempts} times"
                    )
                    raise RuntimeError(msg)

                time.sleep(1)
                continue

            if status == "Success":
                break

            msg = f"{self.report_request_name} failed ({status})"
            raise RuntimeError(msg)

        download_url = report_request_status["ReportDownloadUrl"]

        if not download_url:
            self.logger.info(
                "No %s data available for account: %s",
                self.report_request_name,
                account_id,
            )
            return

        report_file.parent.mkdir(exist_ok=True)

        with (
            self.requests_session.get(download_url, stream=True) as r,
            report_file.open("wb") as f,
        ):
            self.validate_response(r)
            self.logger.info(
                "Downloading: %s (%.1f MB)",
                f.name,
                int(r.headers["Content-Length"]) / (1000**2),
            )

            for chunk in r.iter_content(chunk_size=8192):  # 8 KB chunks
                if chunk:  # skip keep-alive chunks
                    f.write(chunk)

        with zipfile.ZipFile(report_file) as z:
            self.logger.info(
                "Processing file '%s' for account: %s",
                f.name,
                account_id,
            )

            for info in z.infolist():
                with (
                    z.open(info.filename) as f,
                    io.TextIOWrapper(f, encoding="utf-8-sig") as f,
                ):
                    self.logger.info(
                        "Processing zipped file: %s (%.1fMB)",
                        f.name,
                        info.file_size / (1000**2),
                    )
                    yield from csv.DictReader(f)

    @override
    def post_process(self, row, context=None):
        for k, v in row.copy().items():
            if v == "":
                row[k] = None

        return row

    @cached_property
    def columns(self):
        # only selected properties
        column_names = {
            p
            for p in self.schema["properties"]
            if self.metadata[("properties", p)].selected is not False
        }

        # resolve restricted column combinations
        # https://learn.microsoft.com/en-us/advertising/guides/reports?view=bingads-13#columnrestrictions
        for restrictions in self.column_restrictions:
            attributes, impression_share_performance_statistics = restrictions

            # preserve attributes over impression_share_performance_statistics
            if any(a in column_names for a in attributes):
                column_names -= impression_share_performance_statistics
            elif any(
                isps in column_names for isps in impression_share_performance_statistics
            ):
                column_names -= attributes

        return column_names


class AdGroupDailyPerformanceStream(_DailyPerformanceReportStream):
    """Define ad group daily performance stream."""

    name = "ad_group_daily_performance"

    schema = th.PropertiesList(
        th.Property("AbsoluteTopImpressionRatePercent", th.StringType),
        th.Property("AbsoluteTopImpressionShareLostToBudgetPercent", th.StringType),
        th.Property("AbsoluteTopImpressionShareLostToRankPercent", th.StringType),
        th.Property("AbsoluteTopImpressionSharePercent", th.StringType),
        th.Property("AccountId", th.StringType),
        th.Property("AccountName", th.StringType),
        th.Property("AccountNumber", th.StringType),
        th.Property("AccountStatus", th.StringType),
        th.Property("AdDistribution", th.StringType),
        th.Property("AdGroupId", th.StringType),
        th.Property("AdGroupLabels", th.StringType),
        th.Property("AdGroupName", th.StringType),
        th.Property("AdGroupType", th.StringType),
        th.Property("AdRelevance", th.StringType),
        th.Property("AllConversionRate", th.StringType),
        th.Property("AllConversions", th.StringType),
        th.Property("AllConversionsQualified", th.StringType),
        th.Property("AllCostPerConversion", th.StringType),
        th.Property("AllReturnOnAdSpend", th.StringType),
        th.Property("AllRevenue", th.StringType),
        th.Property("AllRevenuePerConversion", th.StringType),
        th.Property("Assists", th.StringType),
        th.Property("AudienceImpressionLostToBudgetPercent", th.StringType),
        th.Property("AudienceImpressionLostToRankPercent", th.StringType),
        th.Property("AudienceImpressionSharePercent", th.StringType),
        th.Property("AverageCpc", th.StringType),
        th.Property("AverageCpm", th.StringType),
        th.Property("AverageCPV", th.StringType),
        th.Property("AveragePosition", th.StringType),
        th.Property("AverageWatchTimePerImpression", th.StringType),
        th.Property("AverageWatchTimePerVideoView", th.StringType),
        th.Property("BaseCampaignId", th.StringType),
        th.Property("BidMatchType", th.StringType),
        th.Property("CampaignId", th.StringType),
        th.Property("CampaignName", th.StringType),
        th.Property("CampaignStatus", th.StringType),
        th.Property("CampaignType", th.StringType),
        th.Property("Clicks", th.StringType),
        th.Property("ClickSharePercent", th.StringType),
        th.Property("CompletedVideoViews", th.StringType),
        th.Property("ConversionRate", th.StringType),
        th.Property("Conversions", th.StringType),
        th.Property("ConversionsQualified", th.StringType),
        th.Property("CostPerAssist", th.StringType),
        th.Property("CostPerConversion", th.StringType),
        th.Property("CostPerInstall", th.StringType),
        th.Property("CostPerSale", th.StringType),
        th.Property("Ctr", th.StringType),
        th.Property("CurrencyCode", th.StringType),
        th.Property("CustomerId", th.StringType),
        th.Property("CustomerName", th.StringType),
        th.Property("CustomParameters", th.StringType),
        th.Property("DeliveredMatchType", th.StringType),
        th.Property("DeviceOS", th.StringType),
        th.Property("DeviceType", th.StringType),
        th.Property("ExactMatchImpressionSharePercent", th.StringType),
        th.Property("ExpectedCtr", th.StringType),
        th.Property("FinalUrlSuffix", th.StringType),
        th.Property("Goal", th.StringType),
        th.Property("GoalId", th.StringType),
        th.Property("GoalType", th.StringType),
        th.Property("HistoricalAdRelevance", th.StringType),
        th.Property("HistoricalExpectedCtr", th.StringType),
        th.Property("HistoricalLandingPageExperience", th.StringType),
        th.Property("HistoricalQualityScore", th.StringType),
        th.Property("ImpressionLostToBudgetPercent", th.StringType),
        th.Property("ImpressionLostToRankAggPercent", th.StringType),
        th.Property("Impressions", th.StringType),
        th.Property("ImpressionSharePercent", th.StringType),
        th.Property("Installs", th.StringType),
        th.Property("LandingPageExperience", th.StringType),
        th.Property("Language", th.StringType),
        th.Property("Network", th.StringType),
        th.Property("PhoneCalls", th.StringType),
        th.Property("PhoneImpressions", th.StringType),
        th.Property("Ptr", th.StringType),
        th.Property("QualityScore", th.StringType),
        th.Property("RelativeCtr", th.StringType),
        th.Property("ReturnOnAdSpend", th.StringType),
        th.Property("Revenue", th.StringType),
        th.Property("RevenuePerAssist", th.StringType),
        th.Property("RevenuePerConversion", th.StringType),
        th.Property("RevenuePerInstall", th.StringType),
        th.Property("RevenuePerSale", th.StringType),
        th.Property("Sales", th.StringType),
        th.Property("Spend", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("TimePeriod", th.StringType),
        th.Property("TopImpressionRatePercent", th.StringType),
        th.Property("TopImpressionShareLostToBudgetPercent", th.StringType),
        th.Property("TopImpressionShareLostToRankPercent", th.StringType),
        th.Property("TopImpressionSharePercent", th.StringType),
        th.Property("TopVsOther", th.StringType),
        th.Property("TotalWatchTimeInMS", th.StringType),
        th.Property("TrackingTemplate", th.StringType),
        th.Property("VideoCompletionRate", th.StringType),
        th.Property("VideoViews", th.StringType),
        th.Property("VideoViewsAt25Percent", th.StringType),
        th.Property("VideoViewsAt50Percent", th.StringType),
        th.Property("VideoViewsAt75Percent", th.StringType),
        th.Property("ViewThroughConversions", th.StringType),
        th.Property("ViewThroughConversionsQualified", th.StringType),
        th.Property("ViewThroughRate", th.StringType),
        th.Property("ViewThroughRevenue", th.StringType),
    ).to_dict()

    # https://learn.microsoft.com/en-us/advertising/reporting-service/adgroupperformancereportrequest?view=bingads-13&tabs=json
    report_request_name = "AdGroupPerformanceReportRequest"

    column_restrictions = (
        (
            {
                "BidMatchType",
                "DeviceOS",
                "Goal",
                "GoalType",
                "TopVsOther",
            },
            {
                "AbsoluteTopImpressionRatePercent",
                "AbsoluteTopImpressionShareLostToBudgetPercent",
                "AbsoluteTopImpressionShareLostToRankPercent",
                "AbsoluteTopImpressionSharePercent",
                "AudienceImpressionLostToBudgetPercent",
                "AudienceImpressionLostToRankPercent",
                "AudienceImpressionSharePercent",
                "ClickSharePercent",
                "ExactMatchImpressionSharePercent",
                "ImpressionLostToAdRelevancePercent",
                "ImpressionLostToBidPercent",
                "ImpressionLostToBudgetPercent",
                "ImpressionLostToExpectedCtrPercent",
                "ImpressionLostToRankAggPercent",
                "ImpressionLostToRankPercent",
                "ImpressionSharePercent",
                "RelativeCtr",
                "TopImpressionRatePercent",
                "TopImpressionShareLostToBudgetPercent",
                "TopImpressionShareLostToRankPercent",
                "TopImpressionSharePercent",
            },
        ),
        (
            {
                "CustomerId",
                "CustomerName",
                "DeliveredMatchType",
            },
            {
                "AudienceImpressionLostToBudgetPercent",
                "AudienceImpressionLostToRankPercent",
                "AudienceImpressionSharePercent",
                "RelativeCtr",
            },
        ),
    )

    @override
    @cached_property
    def primary_keys(self):
        return (*super().primary_keys, "AdGroupId")


class AdDailyPerformanceStream(_DailyPerformanceReportStream):
    """Define ad daily performance stream."""

    name = "ad_daily_performance"

    schema = th.PropertiesList(
        th.Property("AbsoluteTopImpressionRatePercent", th.StringType),
        th.Property("AccountId", th.StringType),
        th.Property("AccountName", th.StringType),
        th.Property("AccountNumber", th.StringType),
        th.Property("AccountStatus", th.StringType),
        th.Property("AdDescription", th.StringType),
        th.Property("AdDescription2", th.StringType),
        th.Property("AdDistribution", th.StringType),
        th.Property("AdGroupId", th.StringType),
        th.Property("AdGroupName", th.StringType),
        th.Property("AdGroupStatus", th.StringType),
        th.Property("AdId", th.StringType),
        th.Property("AdLabels", th.StringType),
        th.Property("AdStatus", th.StringType),
        th.Property("AdStrength", th.StringType),
        th.Property("AdStrengthActionItems", th.StringType),
        th.Property("AdTitle", th.StringType),
        th.Property("AdType", th.StringType),
        th.Property("AllConversionRate", th.StringType),
        th.Property("AllConversions", th.StringType),
        th.Property("AllConversionsQualified", th.StringType),
        th.Property("AllCostPerConversion", th.StringType),
        th.Property("AllReturnOnAdSpend", th.StringType),
        th.Property("AllRevenue", th.StringType),
        th.Property("AllRevenuePerConversion", th.StringType),
        th.Property("Assists", th.StringType),
        th.Property("AverageCpc", th.StringType),
        th.Property("AverageCpm", th.StringType),
        th.Property("AverageCPV", th.StringType),
        th.Property("AveragePosition", th.StringType),
        th.Property("AverageWatchTimePerImpression", th.StringType),
        th.Property("AverageWatchTimePerVideoView", th.StringType),
        th.Property("BaseCampaignId", th.StringType),
        th.Property("BidMatchType", th.StringType),
        th.Property("BusinessName", th.StringType),
        th.Property("CampaignId", th.StringType),
        th.Property("CampaignName", th.StringType),
        th.Property("CampaignStatus", th.StringType),
        th.Property("CampaignType", th.StringType),
        th.Property("Clicks", th.StringType),
        th.Property("CompletedVideoViews", th.StringType),
        th.Property("ConversionRate", th.StringType),
        th.Property("Conversions", th.StringType),
        th.Property("ConversionsQualified", th.StringType),
        th.Property("CostPerAssist", th.StringType),
        th.Property("CostPerConversion", th.StringType),
        th.Property("Ctr", th.StringType),
        th.Property("CurrencyCode", th.StringType),
        th.Property("CustomerId", th.StringType),
        th.Property("CustomerName", th.StringType),
        th.Property("CustomParameters", th.StringType),
        th.Property("DeliveredMatchType", th.StringType),
        th.Property("DestinationUrl", th.StringType),
        th.Property("DeviceOS", th.StringType),
        th.Property("DeviceType", th.StringType),
        th.Property("DisplayUrl", th.StringType),
        th.Property("FinalAppUrl", th.StringType),
        th.Property("FinalMobileUrl", th.StringType),
        th.Property("FinalUrl", th.StringType),
        th.Property("FinalUrlSuffix", th.StringType),
        th.Property("Goal", th.StringType),
        th.Property("GoalId", th.StringType),
        th.Property("GoalType", th.StringType),
        th.Property("Headline", th.StringType),
        th.Property("Impressions", th.StringType),
        th.Property("Language", th.StringType),
        th.Property("LongHeadline", th.StringType),
        th.Property("Network", th.StringType),
        th.Property("Path1", th.StringType),
        th.Property("Path2", th.StringType),
        th.Property("ReturnOnAdSpend", th.StringType),
        th.Property("Revenue", th.StringType),
        th.Property("RevenuePerAssist", th.StringType),
        th.Property("RevenuePerConversion", th.StringType),
        th.Property("Spend", th.StringType),
        th.Property("TimePeriod", th.StringType),
        th.Property("TitlePart1", th.StringType),
        th.Property("TitlePart2", th.StringType),
        th.Property("TitlePart3", th.StringType),
        th.Property("TopImpressionRatePercent", th.StringType),
        th.Property("TopVsOther", th.StringType),
        th.Property("TotalWatchTimeInMS", th.StringType),
        th.Property("TrackingTemplate", th.StringType),
        th.Property("VideoCompletionRate", th.StringType),
        th.Property("VideoViews", th.StringType),
        th.Property("VideoViewsAt25Percent", th.StringType),
        th.Property("VideoViewsAt50Percent", th.StringType),
        th.Property("VideoViewsAt75Percent", th.StringType),
        th.Property("ViewThroughConversions", th.StringType),
        th.Property("ViewThroughConversionsQualified", th.StringType),
        th.Property("ViewThroughRate", th.StringType),
        th.Property("ViewThroughRevenue", th.StringType),
    ).to_dict()

    # https://learn.microsoft.com/en-us/advertising/reporting-service/adperformancereportrequest?view=bingads-13&tabs=json
    report_request_name = "AdPerformanceReportRequest"

    @override
    @cached_property
    def primary_keys(self):
        return (*super().primary_keys, "AdId")


class KeywordDailyPerformanceStream(_DailyPerformanceReportStream):
    """Define kewword daily performance stream."""

    name = "keyword_daily_performance"

    schema = th.PropertiesList(
        th.Property("AbsoluteTopImpressionRatePercent", th.StringType),
        th.Property("AccountId", th.StringType),
        th.Property("AccountName", th.StringType),
        th.Property("AccountNumber", th.StringType),
        th.Property("AccountStatus", th.StringType),
        th.Property("AdDistribution", th.StringType),
        th.Property("AdGroupId", th.StringType),
        th.Property("AdGroupName", th.StringType),
        th.Property("AdGroupStatus", th.StringType),
        th.Property("AdId", th.StringType),
        th.Property("AdRelevance", th.StringType),
        th.Property("AdType", th.StringType),
        th.Property("AllConversionRate", th.StringType),
        th.Property("AllConversions", th.StringType),
        th.Property("AllConversionsQualified", th.StringType),
        th.Property("AllCostPerConversion", th.StringType),
        th.Property("AllReturnOnAdSpend", th.StringType),
        th.Property("AllRevenue", th.StringType),
        th.Property("AllRevenuePerConversion", th.StringType),
        th.Property("Assists", th.StringType),
        th.Property("AverageCpc", th.StringType),
        th.Property("AverageCpm", th.StringType),
        th.Property("AveragePosition", th.StringType),
        th.Property("BaseCampaignId", th.StringType),
        th.Property("BidMatchType", th.StringType),
        th.Property("BidStrategyType", th.StringType),
        th.Property("CampaignId", th.StringType),
        th.Property("CampaignName", th.StringType),
        th.Property("CampaignStatus", th.StringType),
        th.Property("Clicks", th.StringType),
        th.Property("ConversionRate", th.StringType),
        th.Property("Conversions", th.StringType),
        th.Property("ConversionsQualified", th.StringType),
        th.Property("CostPerAssist", th.StringType),
        th.Property("CostPerConversion", th.StringType),
        th.Property("Ctr", th.StringType),
        th.Property("CurrencyCode", th.StringType),
        th.Property("CurrentMaxCpc", th.StringType),
        th.Property("CustomParameters", th.StringType),
        th.Property("DeliveredMatchType", th.StringType),
        th.Property("DestinationUrl", th.StringType),
        th.Property("DeviceOS", th.StringType),
        th.Property("DeviceType", th.StringType),
        th.Property("ExpectedCtr", th.StringType),
        th.Property("FinalAppUrl", th.StringType),
        th.Property("FinalMobileUrl", th.StringType),
        th.Property("FinalUrl", th.StringType),
        th.Property("FinalUrlSuffix", th.StringType),
        th.Property("FirstPageBid", th.StringType),
        th.Property("Goal", th.StringType),
        th.Property("GoalId", th.StringType),
        th.Property("GoalType", th.StringType),
        th.Property("HistoricalAdRelevance", th.StringType),
        th.Property("HistoricalExpectedCtr", th.StringType),
        th.Property("HistoricalLandingPageExperience", th.StringType),
        th.Property("HistoricalQualityScore", th.StringType),
        th.Property("Impressions", th.StringType),
        th.Property("Keyword", th.StringType),
        th.Property("KeywordId", th.StringType),
        th.Property("KeywordLabels", th.StringType),
        th.Property("KeywordStatus", th.StringType),
        th.Property("LandingPageExperience", th.StringType),
        th.Property("Language", th.StringType),
        th.Property("Mainline1Bid", th.StringType),
        th.Property("MainlineBid", th.StringType),
        th.Property("Network", th.StringType),
        th.Property("QualityImpact", th.StringType),
        th.Property("QualityScore", th.StringType),
        th.Property("ReturnOnAdSpend", th.StringType),
        th.Property("Revenue", th.StringType),
        th.Property("RevenuePerAssist", th.StringType),
        th.Property("RevenuePerConversion", th.StringType),
        th.Property("Spend", th.StringType),
        th.Property("TimePeriod", th.StringType),
        th.Property("TopImpressionRatePercent", th.StringType),
        th.Property("TopVsOther", th.StringType),
        th.Property("TrackingTemplate", th.StringType),
        th.Property("ViewThroughConversions", th.StringType),
        th.Property("ViewThroughConversionsQualified", th.StringType),
        th.Property("ViewThroughRevenue", th.StringType),
    ).to_dict()

    # https://learn.microsoft.com/en-us/advertising/reporting-service/keywordperformancereportrequest?view=bingads-13&tabs=json
    report_request_name = "KeywordPerformanceReportRequest"

    @override
    @cached_property
    def primary_keys(self):
        return (*super().primary_keys, "KeywordId")
