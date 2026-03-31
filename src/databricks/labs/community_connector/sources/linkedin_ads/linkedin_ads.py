"""LinkedIn Ads Lakeflow community connector."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.linkedin_ads.client import (
    LinkedinAdsClient,
    LinkedinAdsClientConfig,
    _as_list,
)
from databricks.labs.community_connector.sources.linkedin_ads.schemas import (
    DEFAULT_AD_ANALYTICS_FIELDS,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    get_analytics_pivot_for_table,
    list_supported_tables,
)


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _iso_date(d: date) -> str:
    return d.isoformat()


def _parse_iso_date(s: str) -> date:
    return date.fromisoformat(s)


def _extract_element_date(e: dict[str, Any], default_day: date) -> str:
    """Best-effort parse of LinkedIn adAnalytics element date."""
    start = _safe_get_nested(e, ["dateRange", "start"])
    if isinstance(start, dict):
        year = start.get("year")
        month = start.get("month")
        day = start.get("day")
        if isinstance(year, int) and isinstance(month, int) and isinstance(day, int):
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                pass
    return default_day.isoformat()


def _safe_get_nested(obj: dict[str, Any], path: list[str]) -> Any:
    cur: Any = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


class LinkedinAdsLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for LinkedIn Ads (Marketing) APIs."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        access_token = options.get("access_token") or options.get("token") or ""
        if not access_token:
            raise ValueError("Missing required option: access_token")

        linkedin_version = options.get("linkedin_version") or options.get("Linkedin-Version")
        cfg = LinkedinAdsClientConfig(
            access_token=access_token,
            linkedin_version=linkedin_version or LinkedinAdsClientConfig.linkedin_version,
            timeout_seconds=float(options.get("timeout_seconds", "60")),
            max_retries=int(options.get("max_retries", "8")),
            initial_backoff_seconds=float(options.get("initial_backoff_seconds", "1.0")),
        )
        self._client = LinkedinAdsClient(cfg)

        # Cap incremental reads to prevent chasing new data within a single trigger.
        # Analytics are daily; we cap at UTC "today" (exclusive).
        self._init_today = _utc_today()

    # ---------------- LakeflowConnect required methods ----------------

    def list_tables(self) -> list[str]:
        return list_supported_tables()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        metadata = self.read_table_metadata(table_name, table_options)
        ingestion_type = metadata.get("ingestion_type")

        if table_name.startswith("ad_analytics_"):
            return self._read_ad_analytics_daily(table_name, start_offset, table_options)

        if ingestion_type == "snapshot":
            return self._read_snapshot_entity(table_name, table_options)

        # Default: treat unknown as snapshot for safety.
        return self._read_snapshot_entity(table_name, table_options)

    # ---------------- Internal helpers ----------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )

    def _read_snapshot_entity(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # Endpoints here are best-effort defaults; can be overridden via options.
        # We keep payloads forward-compatible by adding raw_json in the client.
        endpoint_overrides = {
            "ad_accounts": self.options.get("ad_accounts_path", "/adAccounts"),
            "campaign_groups": self.options.get("campaign_groups_path", "/adCampaignGroups"),
            "campaigns": self.options.get("campaigns_path", "/adCampaigns"),
            "creatives": self.options.get("creatives_path", "/creatives"),
        }
        path = endpoint_overrides[table_name]

        # Some endpoints may require q=search; default to finder search and paginate.
        base_params: dict[str, Any] = {}
        if table_name != "ad_accounts":
            # Prefer scoping by accounts if available.
            account_urns = self._get_account_urns(table_options)
            if account_urns and table_name in ("campaign_groups", "campaigns", "creatives"):
                # Common Rest.li filter shapes; may vary. Keep these as optional best-effort.
                base_params["accounts"] = f"List({','.join(account_urns)})"

        records = []
        for elem in self._client.iter_offset_paginated(
            path,
            params={**base_params, "q": "search"},
            page_size=int(table_options.get("page_size", "100")),
        ):
            rec = self._client.normalize_record(elem)
            self._add_common_entity_fields(table_name, rec)
            records.append(rec)

        return iter(records), {}

    def _add_common_entity_fields(self, table_name: str, rec: dict[str, Any]) -> None:
        # Best-effort field normalization across slightly different payloads.
        if table_name == "ad_accounts":
            if "name" not in rec:
                rec["name"] = rec.get("localizedName") or rec.get("accountName")
            if "currency" not in rec:
                rec["currency"] = rec.get("currency")
            if "status" not in rec:
                rec["status"] = rec.get("status")
            rec["reference"] = rec.get("reference") or rec.get("referenceEntity")

        if table_name in ("campaign_groups", "campaigns", "creatives"):
            if "account" not in rec:
                rec["account"] = rec.get("account") or rec.get("accountUrn") or rec.get(
                    "sponsoredAccount"
                )

        if table_name == "campaigns" and "campaign_group" not in rec:
            rec["campaign_group"] = rec.get("campaignGroup") or rec.get("campaignGroupUrn")

        if table_name == "creatives" and "campaign" not in rec:
            rec["campaign"] = rec.get("campaign") or rec.get("campaignUrn")

        # Try to extract a stable last-modified timestamp when present.
        # Common shape: changeAuditStamps.lastModified.time (epoch millis)
        ts = _safe_get_nested(rec, ["changeAuditStamps", "lastModified", "time"])
        if ts is None:
            ts = _safe_get_nested(rec, ["changeAuditStamps", "created", "time"])
        if ts is not None:
            rec["last_modified_at"] = ts

    def _get_account_urns(self, table_options: dict[str, str]) -> list[str]:
        # Prefer explicit options; otherwise attempt discovery.
        urns = _as_list(table_options.get("account_urns")) or _as_list(self.options.get("account_urns"))
        if urns:
            return urns
        discovered = self._client.get_accessible_account_urns()
        return discovered

    def _read_ad_analytics_daily(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        pivot = get_analytics_pivot_for_table(table_name)

        # Cursor protocol: offset contains last completed date (YYYY-MM-DD).
        # We produce one-day batches by default.
        lookback_days = int(table_options.get("lookback_days", self.options.get("lookback_days", "7")))
        window_days = int(table_options.get("window_days", self.options.get("window_days", "1")))
        window_days = max(1, min(31, window_days))

        start_date_opt = table_options.get("start_date") or self.options.get("start_date")
        if start_date_opt:
            lower_bound = _parse_iso_date(start_date_opt)
        else:
            lower_bound = self._init_today - timedelta(days=30)

        if start_offset and start_offset.get("date_cursor"):
            checkpoint = _parse_iso_date(start_offset["date_cursor"])
            # Reprocess lookback window by moving start back; offset will still advance forward.
            cur_start = max(lower_bound, checkpoint - timedelta(days=lookback_days))
        else:
            cur_start = lower_bound

        # Cap end at init_today-1 (yesterday) to avoid incomplete same-day data.
        max_day = self._init_today - timedelta(days=1)
        if cur_start > max_day:
            return iter([]), start_offset or {"date_cursor": _iso_date(max_day)}

        cur_end = min(max_day, cur_start + timedelta(days=window_days - 1))

        fields = _as_list(table_options.get("ad_analytics_fields")) or _as_list(
            self.options.get("ad_analytics_fields")
        )
        if not fields:
            fields = list(DEFAULT_AD_ANALYTICS_FIELDS)

        account_urns = self._get_account_urns(table_options)

        elements = self._client.ad_analytics_daily(
            pivot=pivot,
            start_date=_iso_date(cur_start),
            end_date=_iso_date(cur_end),
            fields=fields,
            account_urns=account_urns or None,
        )

        records: list[dict[str, Any]] = []
        for e in elements:
            rec = self._client.normalize_record(e)
            # Extract pivot identifiers if present; payloads vary by pivot.
            rec["date"] = _extract_element_date(e, cur_start)
            rec["pivot"] = pivot

            pivot_value = (
                e.get("pivotValue")
                or e.get("pivotValues")
                or e.get("creative")
                or e.get("campaign")
                or e.get("campaignGroup")
                or e.get("account")
            )
            if isinstance(pivot_value, list) and pivot_value:
                pivot_value = pivot_value[0]
            rec["pivot_value"] = pivot_value if isinstance(pivot_value, str) else ""

            # Best-effort standard dimension columns.
            for k in ("account", "campaignGroup", "campaign", "creative"):
                v = e.get(k)
                if isinstance(v, str):
                    if k == "campaignGroup":
                        rec["campaign_group"] = v
                    else:
                        rec[k] = v

            # Best-effort metric flattening.
            for metric_key in ("impressions", "clicks"):
                v = e.get(metric_key)
                if isinstance(v, int):
                    rec[metric_key] = v
                elif isinstance(v, str) and v.isdigit():
                    rec[metric_key] = int(v)

            cost = e.get("costInLocalCurrency")
            if cost is not None:
                rec["spend_local"] = str(cost)

            records.append(rec)

        end_offset = {"date_cursor": _iso_date(cur_end)}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

