"""Amazon Ads connector for ingesting data from Amazon Advertising APIs."""

import time
from datetime import datetime, timezone
from typing import Iterator

import requests
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.amazon_ads.amazon_ads_schemas import (
    INGESTION_TYPE_OVERRIDES,
    TABLE_SCHEMAS,
    TABLE_METADATA,
    RETRIABLE_STATUS_CODES,
    MAX_RETRIES,
    INITIAL_BACKOFF,
    REQUEST_TIMEOUT,
)


class AmazonAdsLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Amazon Advertising APIs."""

    # API endpoints for each region
    REGION_ENDPOINTS = {
        "NA": "https://advertising-api.amazon.com",
        "EU": "https://advertising-api-eu.amazon.com",
        "FE": "https://advertising-api-fe.amazon.com",
    }

    OAUTH_TOKEN_ENDPOINT = "https://api.amazon.com/auth/o2/token"

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")
        self.refresh_token = options.get("refresh_token")
        self.region = options.get("region", "NA")

        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError("client_id, client_secret, and refresh_token are required")

        self.base_url = self.REGION_ENDPOINTS.get(self.region, self.REGION_ENDPOINTS["NA"])
        self._access_token = None
        self._token_expires_at = 0
        self._profiles = None  # Cached profiles list

        # Cap cursors at init time to prevent infinite fetching
        self._init_ts = datetime.now(timezone.utc).isoformat()

    def _get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        now = time.time()
        if self._access_token and now < self._token_expires_at:
            return self._access_token

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        resp = requests.post(
            self.OAUTH_TOKEN_ENDPOINT,
            data=payload,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        token_data = resp.json()

        self._access_token = token_data["access_token"]
        # Refresh 5 minutes before expiration
        self._token_expires_at = now + token_data.get("expires_in", 3600) - 300

        return self._access_token

    def _request_with_retry(self, method: str, path: str, **kwargs) -> requests.Response:
        """Issue an API request with exponential backoff retry logic."""
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            token = self._get_access_token()
            headers = kwargs.pop("headers", {})
            headers.update({
                "Authorization": f"Bearer {token}",
                "Amazon-Advertising-API-ClientId": self.client_id,
            })

            url = f"{self.base_url}{path}"
            if method == "GET":
                resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, **kwargs)
            elif method == "POST":
                resp = requests.post(url, headers=headers, timeout=REQUEST_TIMEOUT, **kwargs)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2

        return resp

    def _get_profiles(self) -> list[dict]:
        """Fetch and cache the list of profiles."""
        if self._profiles is not None:
            return self._profiles

        resp = self._request_with_retry(
            "GET",
            "/v2/profiles",
            params={"profileTypeFilter": "seller,vendor"}
        )
        resp.raise_for_status()
        self._profiles = resp.json().get("profiles", [])
        return self._profiles

    def list_tables(self) -> list[str]:
        """Return the list of discoverable tables."""
        # First, try to get profiles from the API
        try:
            profiles = self._get_profiles()
            if not profiles:
                # If no profiles, return static tables that don't need profiles
                return list(TABLE_METADATA.keys())
        except Exception:
            # If profile discovery fails, return static tables
            pass

        # For now, return static table list. In a production implementation,
        # we could make this dynamic per profile.
        return list(TABLE_METADATA.keys())

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """Return the Spark schema for a table."""
        self._validate_table(table_name)

        if table_name in TABLE_SCHEMAS:
            return TABLE_SCHEMAS[table_name]

        # For tables not in the hard-coded schema map, raise an error
        # In a production implementation, we could fetch schemas dynamically
        raise ValueError(f"Schema not available for table '{table_name}'")

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Return metadata for a table."""
        self._validate_table(table_name)

        if table_name not in TABLE_METADATA:
            raise ValueError(f"Metadata not available for table '{table_name}'")

        metadata = dict(TABLE_METADATA[table_name])

        # Apply ingestion type overrides if present
        if table_name in INGESTION_TYPE_OVERRIDES:
            metadata["ingestion_type"] = INGESTION_TYPE_OVERRIDES[table_name]

        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read records from a table."""
        self._validate_table(table_name)

        metadata = self.read_table_metadata(table_name, table_options)
        ingestion_type = metadata.get("ingestion_type", "snapshot")

        if ingestion_type == "snapshot":
            return self._read_snapshot(table_name, table_options)
        elif ingestion_type == "cdc":
            return self._read_cdc(table_name, start_offset, table_options, metadata)
        elif ingestion_type == "append":
            return self._read_append(table_name, start_offset, table_options, metadata)
        elif ingestion_type == "cdc_with_deletes":
            return self._read_cdc(table_name, start_offset, table_options, metadata)
        else:
            raise ValueError(f"Unknown ingestion type: {ingestion_type}")

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read deleted records from a table."""
        self._validate_table(table_name)

        metadata = self.read_table_metadata(table_name, table_options)
        ingestion_type = metadata.get("ingestion_type", "snapshot")

        if ingestion_type != "cdc_with_deletes":
            raise ValueError(
                f"Table '{table_name}' does not support deleted records. "
                f"Only tables with ingestion_type='cdc_with_deletes' support this."
            )

        # For now, Amazon Ads does not have dedicated delete feeds.
        # Treat archived/disabled states as soft-deletes in the read method.
        # Return empty deletes since the API doesn't expose a dedicated delete feed.
        return iter([]), start_offset or {}

    def _validate_table(self, table_name: str) -> None:
        """Validate that a table is supported."""
        supported = self.list_tables()
        if table_name not in supported:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {supported}"
            )

    def _read_snapshot(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read a full snapshot of a table."""
        records = []

        # Route to appropriate API handler based on table name
        if table_name == "profiles":
            records = self._fetch_profiles()
        elif table_name == "portfolios":
            records = self._fetch_portfolios(table_options)
        elif table_name == "sponsored_display_campaigns":
            records = self._fetch_sponsored_display_campaigns(table_options)
        elif table_name == "stores":
            records = self._fetch_stores(table_options)
        else:
            raise ValueError(f"Snapshot read not implemented for table '{table_name}'")

        return iter(records), {}

    def _read_cdc(
        self, table_name: str, start_offset: dict, table_options: dict[str, str], metadata: dict
    ) -> tuple[Iterator[dict], dict]:
        """Read records for CDC (Change Data Capture) tables."""
        cursor_field = metadata.get("cursor_field")
        if not cursor_field:
            raise ValueError(f"No cursor field defined for CDC table '{table_name}'")

        # Short-circuit: if we've already reached init time, no more data
        since = start_offset.get("cursor") if start_offset else None
        if since and since >= self._init_ts:
            return iter([]), start_offset

        max_records = int(table_options.get("max_records_per_batch", "100"))

        # Fetch records based on table type
        records = self._fetch_cdc_records(table_name, since, max_records, table_options)

        if not records:
            return iter([]), start_offset or {}

        # Get the last record's cursor value
        last_cursor = records[-1].get(cursor_field)
        end_offset = {"cursor": last_cursor} if last_cursor else start_offset or {}

        # Check if we've reached the end
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _read_append(
        self, table_name: str, start_offset: dict, table_options: dict[str, str], metadata: dict
    ) -> tuple[Iterator[dict], dict]:
        """Read records for append-only tables."""
        cursor_field = metadata.get("cursor_field")
        if not cursor_field:
            raise ValueError(f"No cursor field defined for append table '{table_name}'")

        # Short-circuit: if we've already reached init time, no more data
        since = start_offset.get("cursor") if start_offset else None
        if since and since >= self._init_ts:
            return iter([]), start_offset

        limit = int(table_options.get("limit", "50"))
        max_records = int(table_options.get("max_records_per_batch", "100"))

        # Fetch records based on table type
        records = self._fetch_append_records(table_name, since, limit, max_records, table_options)

        if not records:
            return iter([]), start_offset or {}

        # Get the last record's cursor value
        last_cursor = records[-1].get(cursor_field)
        end_offset = {"cursor": last_cursor} if last_cursor else start_offset or {}

        # Check if we've reached the end
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _fetch_profiles(self) -> list[dict]:
        """Fetch profiles from the Profiles API."""
        try:
            profiles = self._get_profiles()
            return profiles
        except Exception as e:
            raise RuntimeError(f"Failed to fetch profiles: {e}")

    def _fetch_portfolios(self, table_options: dict[str, str]) -> list[dict]:
        """Fetch portfolios using the POST /portfolios/list endpoint."""
        records = []
        profiles = self._get_profiles()

        for profile in profiles:
            profile_id = profile.get("profileId")
            if not profile_id:
                continue

            # POST /portfolios/list requires profile header
            headers = {"Amazon-Advertising-API-Scope": str(profile_id)}

            resp = self._request_with_retry(
                "POST",
                "/portfolios/list",
                headers=headers,
                json={}
            )

            if resp.status_code == 200:
                portfolios = resp.json()
                for portfolio in portfolios:
                    portfolio["profileId"] = profile_id
                    records.append(portfolio)
            elif resp.status_code != 404:  # 404 means no portfolios
                # Log warning but continue
                pass

        return records

    def _fetch_cdc_records(
        self, table_name: str, since: str, max_records: int, table_options: dict[str, str]
    ) -> list[dict]:
        """Fetch CDC records for a given table."""
        records = []
        metadata = self.read_table_metadata(table_name, table_options)
        cursor_field = metadata.get("cursor_field")
        profiles = self._get_profiles()

        for profile in profiles:
            profile_id = profile.get("profileId")
            if not profile_id:
                continue

            profile_records = self._fetch_records_for_profile(
                table_name, profile_id, since, max_records - len(records), table_options
            )
            
            # Filter by cursor if since is provided
            if since and cursor_field:
                profile_records = [
                    r for r in profile_records
                    if r.get(cursor_field) and r.get(cursor_field) > since
                ]
            
            records.extend(profile_records)

            if len(records) >= max_records:
                records = records[:max_records]
                break

        return records

    def _fetch_append_records(
        self, table_name: str, since: str, limit: int, max_records: int, table_options: dict[str, str]
    ) -> list[dict]:
        """Fetch append-only records for a given table."""
        records = []
        metadata = self.read_table_metadata(table_name, table_options)
        cursor_field = metadata.get("cursor_field")
        profiles = self._get_profiles()

        for profile in profiles:
            profile_id = profile.get("profileId")
            if not profile_id:
                continue

            profile_records = self._fetch_records_for_profile(
                table_name, profile_id, since, max_records - len(records), table_options, limit=limit
            )
            
            # Filter by cursor if since is provided
            if since and cursor_field:
                profile_records = [
                    r for r in profile_records
                    if r.get(cursor_field) and r.get(cursor_field) > since
                ]
            
            records.extend(profile_records)

            if len(records) >= max_records:
                records = records[:max_records]
                break

        return records

    def _fetch_records_for_profile(
        self, table_name: str, profile_id: str, since: str, max_records: int, table_options: dict[str, str], limit: int = None
    ) -> list[dict]:
        """Fetch records for a specific table and profile."""
        records = []

        # Map table names to API endpoints
        endpoint_map = {
            "sponsored_product_campaigns": "/sp/campaigns/list",
            "sponsored_product_ad_groups": "/sp/adGroups/list",
            "sponsored_product_keywords": "/sp/keywords/list",
            "sponsored_product_ads": "/sp/productAds/list",
            "sponsored_product_targetings": "/sp/targets/list",
            "sponsored_brands_campaigns": "/sb/v4/campaigns/list",
            "sponsored_brands_ad_groups": "/sb/v4/adGroups/list",
            "sponsored_brands_keywords": "/sb/v4/keywords/list",
            "sponsored_brands_ads": "/sb/v4/ads/list",
            "sponsored_display_ad_groups": "/sd/adGroups/list",
            "sponsored_display_product_ads": "/sd/productAds/list",
            "sponsored_display_targetings": "/sd/targets/list",
            "brand_safety_profiles": "/brand_safety_profiles",
        }

        if table_name not in endpoint_map:
            return records

        endpoint = endpoint_map[table_name]
        headers = {"Amazon-Advertising-API-Scope": str(profile_id)}

        # Determine if this is a GET or POST endpoint
        if endpoint.startswith("/sp/") and "list" in endpoint:
            # Sponsored Products uses POST
            return self._fetch_list_endpoint_post(endpoint, headers, since, max_records, table_options, profile_id)
        elif endpoint.startswith("/sb/") and "list" in endpoint:
            # Sponsored Brands v4 uses POST
            return self._fetch_list_endpoint_post(endpoint, headers, since, max_records, table_options, profile_id)
        elif endpoint.startswith("/sd/") and "list" in endpoint:
            # Sponsored Display uses POST
            return self._fetch_list_endpoint_post(endpoint, headers, since, max_records, table_options, profile_id)
        elif endpoint.startswith("/brand_safety"):
            # Brand Safety uses GET
            return self._fetch_list_endpoint_get(endpoint, headers, since, max_records, limit)
        else:
            return records

    def _fetch_list_endpoint_post(
        self, endpoint: str, headers: dict, since: str, max_records: int, table_options: dict[str, str], profile_id: str
    ) -> list[dict]:
        """Fetch records from a POST list endpoint."""
        records = []
        page = 0
        
        # Map endpoints to their response keys
        endpoint_response_keys = {
            "/sp/campaigns/list": "campaigns",
            "/sp/adGroups/list": "adGroups",
            "/sp/keywords/list": "keywords",
            "/sp/productAds/list": "productAds",
            "/sp/targets/list": "targets",
            "/sb/v4/campaigns/list": "campaigns",
            "/sb/v4/adGroups/list": "adGroups",
            "/sb/v4/keywords/list": "keywords",
            "/sb/v4/ads/list": "ads",
            "/sd/adGroups/list": "adGroups",
            "/sd/productAds/list": "productAds",
            "/sd/targets/list": "targets",
            "/brand_safety_profiles": "safetyProfiles",
        }
        
        response_key = endpoint_response_keys.get(endpoint, "data")

        while len(records) < max_records:
            body = {
                "startIndex": page,
                "count": min(100, max_records - len(records)),
            }

            # Add filters based on table options
            for key in ["campaignIdFilter", "adGroupIdFilter", "state"]:
                if key in table_options:
                    body[key] = table_options[key]

            resp = self._request_with_retry(
                "POST",
                endpoint,
                headers=headers,
                json=body
            )

            if resp.status_code == 200:
                data = resp.json()
                batch = data.get(response_key, [])

                if not batch:
                    break

                # Add profile ID to each record
                for record in batch:
                    record["profileId"] = profile_id
                    records.append(record)

                # Check for next page
                if len(batch) < body["count"]:
                    break

                page += body["count"]
            else:
                break

        return records

    def _fetch_list_endpoint_get(
        self, endpoint: str, headers: dict, since: str, max_records: int, limit: int = None
    ) -> list[dict]:
        """Fetch records from a GET list endpoint."""
        records = []
        offset = 0
        limit = limit or 100

        while len(records) < max_records:
            params = {
                "offset": offset,
                "limit": min(limit, max_records - len(records))
            }

            resp = self._request_with_retry(
                "GET",
                endpoint,
                params=params,
                headers=headers
            )

            if resp.status_code == 200:
                data = resp.json()
                batch = data if isinstance(data, list) else data.get("adGroups", data.get("productAds", []))

                if not batch:
                    break

                records.extend(batch)

                # Check for next page
                if len(batch) < params["limit"]:
                    break

                offset += params["limit"]
            else:
                break

        return records

    def _fetch_sponsored_display_campaigns(self, table_options: dict[str, str]) -> list[dict]:
        """Fetch Sponsored Display campaigns."""
        records = []
        profiles = self._get_profiles()

        for profile in profiles:
            profile_id = profile.get("profileId")
            if not profile_id:
                continue

            headers = {"Amazon-Advertising-API-Scope": str(profile_id)}
            
            try:
                resp = self._request_with_retry(
                    "GET",
                    "/sd/campaigns",
                    headers=headers,
                    params={"pageSize": 100}
                )

                if resp.status_code == 200:
                    data = resp.json()
                    campaigns = data if isinstance(data, list) else data.get("campaigns", [])
                    
                    for campaign in campaigns:
                        campaign["profileId"] = profile_id
                        records.append(campaign)
            except Exception:
                # Continue if error fetching campaigns for this profile
                pass

        return records

    def _fetch_stores(self, table_options: dict[str, str]) -> list[dict]:
        """Fetch Stores associated with profiles."""
        records = []
        profiles = self._get_profiles()

        for profile in profiles:
            profile_id = profile.get("profileId")
            if not profile_id:
                continue

            try:
                resp = self._request_with_retry(
                    "GET",
                    "/stores/brands",
                    params={"pageSize": 100}
                )

                if resp.status_code == 200:
                    data = resp.json()
                    stores = data if isinstance(data, list) else data.get("stores", [])
                    
                    for store in stores:
                        store["profileId"] = profile_id
                        records.append(store)
            except Exception:
                # Continue if error fetching stores
                pass

        return records
