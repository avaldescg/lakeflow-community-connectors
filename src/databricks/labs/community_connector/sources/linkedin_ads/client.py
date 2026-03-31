"""Thin LinkedIn Marketing (REST) API client with retries and pagination."""

from __future__ import annotations

import json
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Iterable

import requests

from databricks.labs.community_connector.sources.linkedin_ads.constants import (
    DEFAULT_INITIAL_BACKOFF_SECONDS,
    DEFAULT_LINKEDIN_VERSION,
    DEFAULT_MAX_RETRIES,
    DEFAULT_PAGE_SIZE,
    LINKEDIN_REST_BASE_URL,
    RETRIABLE_STATUS_CODES,
)


def _as_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _sleep_seconds_from_retry_after(headers: dict[str, str]) -> float | None:
    retry_after = headers.get("Retry-After")
    if not retry_after:
        return None
    try:
        return max(0.0, float(retry_after))
    except Exception:
        return None


@dataclass(frozen=True)
class LinkedinAdsClientConfig:
    access_token: str
    linkedin_version: str = DEFAULT_LINKEDIN_VERSION
    base_url: str = LINKEDIN_REST_BASE_URL
    max_retries: int = DEFAULT_MAX_RETRIES
    initial_backoff_seconds: float = DEFAULT_INITIAL_BACKOFF_SECONDS
    timeout_seconds: float = 60.0


class LinkedinAdsClient:
    def __init__(self, cfg: LinkedinAdsClientConfig) -> None:
        self._cfg = cfg
        self._session = requests.Session()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._cfg.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "Linkedin-Version": self._cfg.linkedin_version,
            "Accept": "application/json",
        }

    def _request_with_retry(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        data: Any | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> requests.Response:
        url = f"{self._cfg.base_url}{path}"
        headers = dict(self._headers())
        if extra_headers:
            headers.update(extra_headers)

        backoff = self._cfg.initial_backoff_seconds
        last_resp: requests.Response | None = None

        for attempt in range(self._cfg.max_retries):
            try:
                resp = self._session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    data=data,
                    headers=headers,
                    timeout=self._cfg.timeout_seconds,
                )
            except requests.RequestException as e:
                # Network-ish failure; backoff and retry.
                if attempt >= self._cfg.max_retries - 1:
                    raise RuntimeError(f"LinkedIn API request failed: {e}") from e
                time.sleep(backoff)
                backoff *= 2
                continue

            last_resp = resp
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            if attempt >= self._cfg.max_retries - 1:
                return resp

            retry_after = _sleep_seconds_from_retry_after(resp.headers)
            time.sleep(retry_after if retry_after is not None else backoff)
            backoff *= 2

        assert last_resp is not None
        return last_resp

    def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        resp = self._request_with_retry("GET", path, params=params)
        if resp.status_code >= 400:
            raise RuntimeError(f"LinkedIn API error {resp.status_code} for {path}: {resp.text}")
        return resp.json()

    def finder_search(
        self, path: str, *, q: str = "search", params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        p = dict(params or {})
        p["q"] = q
        return self.get_json(path, params=p)

    def iter_offset_paginated(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        elements_key: str = "elements",
        page_size: int = DEFAULT_PAGE_SIZE,
        start: int = 0,
        max_pages: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        """Yield elements from a start/count paginated endpoint.

        LinkedIn REST endpoints often return:
          {"elements": [...], "paging": {"start": 0, "count": 100, "total": 1234}}
        """
        p = dict(params or {})
        cur_start = start
        pages = 0
        while True:
            p["start"] = cur_start
            p["count"] = page_size
            body = self.get_json(path, params=p)
            elems = body.get(elements_key) or []
            if not elems:
                break
            for e in elems:
                yield e
            cur_start += len(elems)

            pages += 1
            if max_pages is not None and pages >= max_pages:
                break

            paging = body.get("paging") or {}
            total = paging.get("total")
            if isinstance(total, int) and cur_start >= total:
                break

    def get_accessible_account_urns(self) -> list[str]:
        # Best-effort: ad account listing patterns vary; try a couple shapes.
        # Primary: /adAccounts?q=search
        try:
            body = self.finder_search("/adAccounts", q="search")
            urns = []
            for elem in body.get("elements") or []:
                urn = elem.get("id") or elem.get("urn") or elem.get("account") or elem.get("accountURN")
                if isinstance(urn, str) and urn:
                    urns.append(urn)
            if urns:
                return urns
        except Exception:
            pass

        # Fallback: some tenants use legacy-ish resources; keep it conservative.
        return []

    def _encode_adanalytics_date_range(self, start_date: str, end_date: str) -> str:
        # Format: (start:(year:2024,month:1,day:1),end:(year:2024,month:1,day:31))
        sy, sm, sd = (int(x) for x in start_date.split("-", 2))
        ey, em, ed = (int(x) for x in end_date.split("-", 2))
        return (
            f"(start:(year:{sy},month:{sm},day:{sd}),"
            f"end:(year:{ey},month:{em},day:{ed}))"
        )

    def _maybe_query_tunnel_get(
        self, path: str, *, params: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Attempt query tunneling (POST with X-HTTP-Method-Override: GET).

        LinkedIn recommends this for long adAnalytics queries to avoid 414.
        We encode the query string in the POST body as form-urlencoded.
        """
        encoded = urllib.parse.urlencode(params, doseq=True)
        resp = self._request_with_retry(
            "POST",
            path,
            data=encoded,
            extra_headers={
                "X-HTTP-Method-Override": "GET",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        if resp.status_code >= 400:
            return None
        return resp.json()

    def ad_analytics_daily(
        self,
        *,
        pivot: str,
        start_date: str,
        end_date: str,
        fields: list[str],
        account_urns: list[str] | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch adAnalytics for a single pivot and inclusive date range (small windows recommended)."""
        params: dict[str, Any] = {
            "q": "analytics",
            "pivot": pivot,
            "timeGranularity": "DAILY",
            "dateRange": self._encode_adanalytics_date_range(start_date, end_date),
        }

        # fields: comma-separated list in query string is commonly accepted.
        if fields:
            params["fields"] = ",".join(fields)

        if extra_params:
            params.update(extra_params)

        if account_urns:
            # Rest.li List(...) syntax for filters.
            params["accounts"] = f"List({','.join(account_urns)})"

        # Try GET first, tunnel on 414.
        resp = self._request_with_retry("GET", "/adAnalytics", params=params)
        if resp.status_code == 414:
            tunneled = self._maybe_query_tunnel_get("/adAnalytics", params=params)
            if tunneled is None:
                raise RuntimeError(f"LinkedIn API 414 and query tunneling failed: {resp.text}")
            body = tunneled
        else:
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"LinkedIn adAnalytics error {resp.status_code}: {resp.text}"
                )
            body = resp.json()

        elements = body.get("elements") or []
        if not isinstance(elements, list):
            return []
        # Ensure elements are dicts.
        return [e for e in elements if isinstance(e, dict)]

    @staticmethod
    def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
        """Make records more stable for downstream ingestion.

        - Ensure 'id' when an obvious urn-like identifier is present.
        - Preserve full payload in 'raw_json' for forward-compat.
        """
        out = dict(record)
        if "id" not in out:
            for k in ("urn", "account", "campaign", "campaignGroup", "creative"):
                v = out.get(k)
                if isinstance(v, str) and v:
                    out["id"] = v
                    break
        out["raw_json"] = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        return out
