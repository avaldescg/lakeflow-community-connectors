# LinkedIn Ads (Marketing) API — Connector Notes

This document summarizes the LinkedIn Marketing APIs needed to ingest **campaign structure** and **ads performance reporting** into Lakeflow Community Connectors.

## Overview

LinkedIn’s Marketing Developer Platform provides APIs for:

- **Campaign management**: ad accounts, campaign groups, campaigns, creatives
- **Reporting**: `adAnalytics` for performance and (optionally) professional demographic pivots
- **Other areas** (often gated by extra access): conversion tracking, lead sync, targeting facets, etc.

Primary reference hub: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/marketing-integrations-overview?view=li-lms-2026-01`

## Authentication

- **Auth type**: OAuth 2.0 (Authorization Code flow is the common pattern for server-side integrations).
- **API base**: REST endpoints under `https://api.linkedin.com/rest/...` for the versioned Marketing APIs.
- **Token**: Bearer access token in `Authorization: Bearer <token>`.

OAuth overview/reference: `https://learn.microsoft.com/en-us/linkedin/shared/authentication/authorization-code-flow?view=li-lms-2026-01`

### Core permissions/scopes

From Reporting docs:

- **`r_ads_reporting`**: retrieve reporting for advertising accounts.

Reporting reference: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2026-03`

Other campaign-management endpoints typically require additional marketing permissions (varies by endpoint + partner access). Plan for the connector to require at least:

- `r_ads_reporting` (reporting)
- plus the relevant read permissions to list accounts/campaigns/creatives (documented on the respective endpoint pages)

## “Tables” (recommended ingestion objects)

Organize the connector into two broad groups:

### A) Core entities (dimension-like)

- `ad_accounts`
- `ad_account_users` (optional but useful for governance)
- `campaign_groups`
- `campaigns`
- `creatives`

These define the graph you’ll join analytics to.

Campaign management docs (starting points):

- Accounts: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts?view=li-lms-2026-01`
- Campaign Groups: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups?view=li-lms-2026-01`
- Campaigns: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns?view=li-lms-2026-01`
- Creatives: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2026-01`

### B) Analytics (fact-like, time-series)

Because `adAnalytics` does **not** paginate and has a **15,000 element** response cap, it’s best modeled as **time-windowed tables** (daily/monthly) with configurable pivots and metrics.

Suggested analytics tables:

- `ad_analytics_creative_daily`
- `ad_analytics_campaign_daily`
- `ad_analytics_campaign_group_daily`
- `ad_analytics_account_daily`
- `ad_analytics_creative_all_time` (optional convenience)

Optional (heavier / privacy-limited / delayed) demographic pivots:

- `ad_analytics_creative_demographics_daily` (e.g., `MEMBER_COMPANY`, `MEMBER_JOB_TITLE`, etc., subject to thresholds)

Reporting docs:

- Reporting overview: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2026-03`
- Reporting schema / metrics tables: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting-schema?view=li-lms-2026-03`

## Key endpoints (high level)

### Entities

Use the Campaign Management APIs to list:

- Ad accounts
- Campaign groups (scoped by account)
- Campaigns (scoped by account and/or group)
- Creatives (scoped by account and/or campaign)

### Reporting (`adAnalytics`)

Endpoint:

- `GET https://api.linkedin.com/rest/adAnalytics`

Finder methods:

- `q=analytics` (single pivot)
- `q=statistics` (up to 3 pivots)
- `q=attributedRevenueMetrics` (revenue attribution metrics)

Core parameters (conceptual):

- `pivot=...` (e.g. `ACCOUNT`, `CAMPAIGN`, `CREATIVE`, and demographic pivots)
- `timeGranularity=...` (`DAILY`, `MONTHLY`, `ALL`)
- `dateRange=(start:(year:...,month:...,day:...),end:(...))`
- facet filters like `campaigns=List(urn:li:sponsoredCampaign:...)`
- `fields=...` (LinkedIn requires explicit metrics selection; otherwise you only get `impressions` and `clicks`)

Important constraints from docs:

- **No pagination**
- **URL length may exceed limits** (use query tunneling to avoid 414)
- **Response limited to 15,000 elements**
- **Demographic metrics delayed (12–24h)** and are approximate / privacy-protected

Query tunneling reference:

- `https://learn.microsoft.com/en-us/linkedin/shared/references/migrations/query-tunneling-migration?view=li-lms-2026-03`

## Incremental strategy (connector-oriented)

### Core entities

LinkedIn entity objects typically include timestamps such as `lastModified` / `changeAuditStamps` (endpoint dependent). A robust connector should:

- Support **snapshot** ingestion for smaller dimensions (accounts, campaign groups)
- Support **CDC** where the API provides a reliable updated timestamp, else fall back to periodic snapshot

### Analytics

Treat analytics as **append-only by time window**, with offsets like:

```json
{"date_cursor": "2026-03-29"}
```

Strategy:

- Configure `timeGranularity=DAILY` (default) and read one day (or small window) per batch.
- Use a **lookback** (e.g. 7–30 days) on each run to accommodate reporting delays and post-facto adjustments.
- Keep request sizes bounded to stay under the 15,000 element cap:
  - narrow date ranges
  - avoid overly wide pivot combinations
  - limit metric list (`fields`) to what’s required

## Rate limits and reliability

Rate limit behavior and quotas can vary by app access tier and endpoint. The connector should implement:

- Retry with exponential backoff for `429` and transient `5xx`
- Respect `Retry-After` when present

## Errors and edge cases

- **Empty responses** can mean either “no activity” or “no access.”
- **414 URI Too Long**: use query tunneling for large `fields`/filters.
- **Demographic pivots** have privacy thresholds (values with low event counts dropped) and top-N limits.

## References

- Marketing overview: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/marketing-integrations-overview?view=li-lms-2026-01`
- Reporting: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2026-03`
- Reporting schema/metrics: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting-schema?view=li-lms-2026-03`
- Accounts: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts?view=li-lms-2026-01`
- Campaign groups: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups?view=li-lms-2026-01`
- Campaigns: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns?view=li-lms-2026-01`
- Creatives: `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2026-01`
- OAuth: `https://learn.microsoft.com/en-us/linkedin/shared/authentication/authorization-code-flow?view=li-lms-2026-01`
- Query tunneling: `https://learn.microsoft.com/en-us/linkedin/shared/references/migrations/query-tunneling-migration?view=li-lms-2026-03`

## Overview
This document summarizes the LinkedIn Marketing (Ads) APIs for a Lakeflow Community Connector named `linkedin_ads`, focused on **ads reporting/analytics ingestion** plus the **core campaign-management entities** required to interpret analytics.

- **Primary API surface**: LinkedIn Marketing **versioned** APIs under `https://api.linkedin.com/rest/` (monthly version header required).
- **Primary reporting endpoint**: `GET /rest/adAnalytics` (performance + professional demographic analytics).
- **Core hierarchy**: **Ad Account → Campaign Group → Campaign → Creative**.

## Auth
### Authentication method
- **OAuth 2.0 (3-legged / member tokens)** is the standard model for Marketing APIs. LinkedIn’s Marketing/Advertising API access generally requires approval in the LinkedIn Developer Portal (Advertising API product) and may require additional programs for some feature areas.

### What the connector should store
For Lakeflow connector implementation (no interactive OAuth UI flow in the connector runtime), store:
- `client_id`
- `client_secret`
- `refresh_token` (if your integration uses refreshable tokens)

At runtime, exchange the refresh token for an access token via LinkedIn’s OAuth token endpoint (see LinkedIn OAuth docs).

### Required headers for Marketing “REST” endpoints
For `https://api.linkedin.com/rest/*` endpoints, include:
- `Authorization: Bearer {access_token}`
- `X-Restli-Protocol-Version: 2.0.0`
- `Linkedin-Version: {YYYYMM}` (required; no default is applied)
- `Content-Type: application/json` (when sending a body)

### Required permissions/scopes (read-only ingestion)
Minimum:
- `r_ads` (read ad accounts / core entities access control)
- `r_ads_reporting` (read ads reporting/analytics)

Optional, depending on tables:
- `rw_conversions` (Conversions API: create/manage conversion rules and upload conversions)
- `r_marketing_leadgen_automation` (Lead Sync API: lead gen forms and responses)
- `rw_dmp_segments` (Matched Audiences / segments)
- `rw_ads` (only if you need write operations; not required for ingestion)

### Access / approval notes
- Marketing permissions typically require explicit approval (Advertising API product access). Some products (Conversions, Lead Sync, Matched Audiences, Audience Insights, Media Planning) require additional approval beyond Advertising API.

## Entities / Tables
This connector can model LinkedIn Marketing resources as “tables”. The catalog below is **comprehensive by API surface**, but should be implemented in priority order.

### Recommended initial table scope (MVP)
Core dimensions (needed to interpret analytics):
- `ad_accounts`
- `ad_account_users`
- `campaign_groups`
- `campaigns`
- `creatives`

Core facts (highest ROI):
- `ad_analytics_by_campaign_daily`
- `ad_analytics_by_creative_daily`
- optionally `ad_analytics_by_account_daily`

### Comprehensive table catalog (organized by priority)
#### Core entities (dimensions)
- **`ad_accounts`**: advertiser accounts (URN + metadata, status, currency, reference org/person).
- **`ad_account_users`**: users/roles on ad accounts (access control; also a useful “who can see what” dimension).
- **`campaign_groups`**: grouping/total budget/run schedule.
- **`campaigns`**: individual campaigns (objective/targeting/budget/run schedule; very high variability).
- **`creatives`**: ad creatives (high variability by format; large volume limits per account).

#### Analytics / reporting (facts; all via `adAnalytics`)
All of these are generated from `GET /rest/adAnalytics` by selecting a `pivot` (or `pivots`) and `timeGranularity`:
- **Core pivots**:
  - `ACCOUNT` → `ad_analytics_by_account_{daily|monthly|all}`
  - `CAMPAIGN_GROUP` → `ad_analytics_by_campaign_group_{daily|monthly|all}`
  - `CAMPAIGN` → `ad_analytics_by_campaign_{daily|monthly|all}`
  - `CREATIVE` → `ad_analytics_by_creative_{daily|monthly|all}`
- **Professional demographics pivots** (examples; subject to privacy thresholds and “top values” truncation):
  - job: `MEMBER_JOB_TITLE`, `MEMBER_JOB_FUNCTION`, `MEMBER_JOB_SENIORITY`
  - company: `MEMBER_COMPANY`, `MEMBER_COMPANY_SIZE`
  - geo (Bing locations): `MEMBER_COUNTRY_V2`, `MEMBER_REGION_V2`, `MEMBER_COUNTY`
  - education: `MEMBER_SCHOOL`, `MEMBER_DEGREE`, `MEMBER_FIELD_OF_STUDY`

Suggested materialization pattern:
- `ad_analytics_{by_campaign|by_creative}_by_{pivot}_daily`

#### Conversions / attribution (optional; separate product access)
- **`conversion_rules`**: conversion rules (`/rest/conversions`) (requires `rw_conversions`).
- **`conversion_events`** (write API): `/rest/conversionEvents` is primarily for uploading; for ingestion you typically care about reporting metrics (pulled via `adAnalytics` conversion-related metrics rather than reading raw conversion events back).
- **Revenue Attribution (CRM-connected)**:
  - `ad_attributed_revenue_metrics_{account|campaign_group|campaign}` via `adAnalytics?q=attributedRevenueMetrics` (only for accounts that have connected CRM to LinkedIn).

#### Lead Sync (optional; separate product access)
- `lead_forms`
- `lead_form_responses`

#### Audiences / targeting (optional; additional programs)
- `ad_targeting_facets`
- `ad_targeting_entities`
- Matched audiences / segments (if approved)
- Audience insights (if approved)

## Endpoints
### Base URL
- Versioned Marketing APIs: `https://api.linkedin.com/rest/`

### Core entities
The exact resource paths vary by entity and are documented in the “Create and Manage …” pages, but the connector will generally need **read/search** operations for:
- **Ad Accounts**: list/search + get.
- **Ad Account Users**: list/search + get.
- **Campaign Groups**: list/search + get (example path pattern in docs: `/rest/adAccounts/{adAccountId}/adCampaignGroups`).
- **Campaigns**: list/search + get (typically nested under ad account in the REST surface).
- **Creatives**: list/search + get (`/rest/creatives` surface is used in newer versions; legacy `adCreativesV2` is deprecated).

### Reporting / analytics
`GET https://api.linkedin.com/rest/adAnalytics`

Finders:
- `q=analytics` (single `pivot=...`)
- `q=statistics` (up to 3 `pivots=List(...)`)
- `q=attributedRevenueMetrics` (revenue attribution; CRM-connected accounts; last-1-year constraints)

Important: LinkedIn documentation states `adAnalytics` **does not support pagination**. You should batch requests by **date ranges** and by **pivot/facet selections**.

### Targeting
- `GET /rest/adTargetingFacets` (discover facets)
- `GET /rest/adTargetingEntities` (enumerate entities; supports multiple finders such as `q=adTargetingFacet`, `q=typeahead`, `q=similarEntities`, `q=urns`)

### Conversions (optional)
- `POST/GET https://api.linkedin.com/rest/conversions` (conversion rules)
- `POST https://api.linkedin.com/rest/conversionEvents` (upload events; supports `X-RestLi-Method: BATCH_CREATE` for batches up to 5,000 events)

## Parameters
### Versioning parameters (required)
- **`Linkedin-Version: YYYYMM` header** is mandatory for versioned endpoints under `https://api.linkedin.com/rest/`.
- **`X-Restli-Protocol-Version: 2.0.0` header** is required to use Rest.li protocol v2 (List(...) syntax, encoded keys, etc.).

If the version header is missing or deprecated, LinkedIn returns structured errors (e.g., `VERSION_MISSING` / `NONEXISTENT_VERSION`).

### Pagination parameters (entity endpoints)
LinkedIn uses multiple pagination styles depending on endpoint and method:
- **Offset pagination**: `start` + `count` (commonly max `count=1000`; some endpoints require paging params if result size is large).
- **Cursor pagination for search** (documented for some search APIs): `pageSize` + `pageToken`, returning `metadata.nextPageToken`.

### `adAnalytics` parameters (reporting)
Common parameters:
- **Pivot**:
  - `q=analytics&pivot=CREATIVE`
  - `q=statistics&pivots=List(CAMPAIGN,CREATIVE)`
- **Time**:
  - `timeGranularity=DAILY|MONTHLY|ALL`
  - `dateRange=(start:(year:2024,month:1,day:1),end:(year:2024,month:1,day:31))`
- **Facets / filters** (examples): `accounts=List(urn:li:sponsoredAccount:...)`, `campaigns=List(...)`, `campaignGroups=List(...)`
- **Fields / metrics**:
  - `fields=...` to request specific metrics; default is typically limited (docs call out `impressions` and `clicks` by default).
  - Maximum **20 metrics** per call.

Operational constraints (from docs):
- **No pagination**.
- **Response limit**: 15,000 elements.
- **Data throttling**: up to **45 million metric values per 5 minutes**, where metric values = `(#metrics in fields) × (#records returned)`.
- **URL length**: complex queries may hit HTTP 414; use **query tunneling** when needed.
- **Retention**:
  - performance data (account/campaign/creative): **10 years**
  - professional demographics pivots: **2 years**

## Incremental Strategy
### Core entities (dimensions)
Recommended ingestion type: **`cdc` (upserts)**.

- **Primary key**: entity URN or numeric ID (normalize to a stable `id` string).
- **Cursor**: where present, use `changeAuditStamps.lastModified.time` (epoch millis) for incremental reads; otherwise run periodic snapshots via search/list endpoints.
- **Deletes**: there is no universal “deleted entities” feed; treat deletes as “missing from future snapshots” unless a specific endpoint provides delete status.

### Analytics (facts)
Recommended ingestion type: **`append`** with **re-processing** (late arriving updates).

- Partition by day or month using `timeGranularity=DAILY` (or `MONTHLY`).
- Maintain a watermark (e.g., last completed day) and **always re-pull a trailing lookback window** (commonly 2–7 days; longer for demographic pivots) because:
  - professional demographics metrics can be delayed **12–24 hours**
  - metrics are approximate and can restate

### Revenue attribution metrics (optional)
If enabled:
- Constrained to the last 1 year; dateRange must be between 30 and 366 days inclusive (per docs).
- Initial availability can be delayed after CRM connection (docs note up to ~72 hours initially).

## Rate Limits
LinkedIn rate limits are enforced per 24-hour window (reset at midnight UTC) and can apply at:
- **Application level**
- **Member (token) level**

LinkedIn does not publish “standard” quotas in docs; you must consult the Developer Portal app analytics for the endpoints you call. Exceeding limits returns **HTTP 429**.

Additional reporting-specific throttling:
- `adAnalytics` imposes a **data volume** throttle (metric-value budget per 5-minute window) in addition to normal request quotas.

## Errors
### Common HTTP status codes to handle
- **400**: validation errors (including pagination misuse and missing required parameters)
- **401**: missing/invalid access token
- **403**: insufficient permission scopes and/or the authenticated member lacks required ad account role
- **404**: resource not found (or inaccessible)
- **414**: URL too long (notably `adAnalytics`; use query tunneling)
- **426**: deprecated/nonexistent `Linkedin-Version` (e.g., `NONEXISTENT_VERSION`)
- **429**: rate limiting / throttling
- **5xx**: transient server errors

### Marketing API structured errors
LinkedIn’s Marketing APIs provide standardized error response payloads (documented for several core endpoints), including:
- `status`, `code`, `message`, `serviceErrorCode`
- `errorDetailType`, `errorDetails` for machine-readable validation failures

## References
### Official LinkedIn / Microsoft Learn (highest confidence)
- Getting access to LinkedIn APIs: `https://learn.microsoft.com/en-us/linkedin/shared/authentication/getting-access?view=li-lms-2022-06`
- Marketing API versioning (required `Linkedin-Version`, latest version header): `https://learn.microsoft.com/en-us/linkedin/marketing/versioning`
- Protocol version header (`X-Restli-Protocol-Version: 2.0.0`): `https://learn.microsoft.com/en-us/linkedin/shared/api-guide/concepts/protocol-version?context=linkedin%2Fmarketing%2Fcontext`
- Ads reporting / `adAnalytics` (limits, throttling, retention, pivots): `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2026-03`
- Rate limiting concepts (429, daily reset, portal analytics): `https://learn.microsoft.com/en-us/linkedin/shared/api-guide/concepts/rate-limits?context=linkedin%2Fcontext&view=li-lms-2022-06`
- Increasing access (permissions table, product approvals, tier notes): `https://learn.microsoft.com/en-us/linkedin/marketing/increasing-access?view=li-lms-2025-06`
- Marketing API error responses (version missing/deprecated, structured details): `https://learn.microsoft.com/en-us/linkedin/marketing/error-responses?view=li-lms-2025-07`
- Conversions API (optional product): `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/conversions-api?view=li-lms-2025-06`
- Campaign groups (pagination details, cursor-based search pagination): `https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups?view=li-lms-2025-06`

### Existing connector implementations (high confidence for “what tables matter”)
- Airbyte LinkedIn Ads connector docs (streams and auth notes): `https://docs.airbyte.com/integrations/sources/linkedin-ads/`

