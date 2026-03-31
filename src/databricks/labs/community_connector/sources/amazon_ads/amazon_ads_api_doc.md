# Amazon Ads API Documentation

Connector-focused API research for `amazon_ads`, covering discoverable endpoint families used for ingestion, plus implementation guidance for auth, pagination, rate limiting, and incremental sync.

## Authorization

- **Preferred auth method**: OAuth 2.0 refresh-token flow (server-to-server).
- **Token endpoint**: `POST https://api.amazon.com/auth/o2/token`
- **Connector-stored credentials**:
  - `client_id`
  - `client_secret`
  - `refresh_token`
- **Required headers for Ads API requests**:
  - `Authorization: Bearer <access_token>`
  - `Amazon-Advertising-API-ClientId: <client_id>`
  - `Amazon-Advertising-API-Scope: <profile_id>` (required for profile-scoped resources, including reporting and most ads entities)
- **Media-type headers**: some versioned endpoints require specific `Accept` and `Content-Type` values (notably Sponsored Products v3 and Sponsored Brands v4 list APIs).

Example token refresh request:

```http
POST https://api.amazon.com/auth/o2/token
Content-Type: application/x-www-form-urlencoded

grant_type=refresh_token&refresh_token=<refresh_token>&client_id=<client_id>&client_secret=<client_secret>
```

Example profile discovery request:

```http
GET https://advertising-api.amazon.com/v2/profiles?profileTypeFilter=seller,vendor
Authorization: Bearer <access_token>
Amazon-Advertising-API-ClientId: <client_id>
```

### Required permissions / roles

Amazon Ads access is role-based at the advertiser account/profile level. In practice, connector integrations commonly require:

- campaign metadata access (for example, campaign view/edit roles),
- reporting permissions (for example, report view/edit roles).

## Object List

The discoverable object inventory is effectively static by API family/version, while object records are dynamic by profile, marketplace, and date windows.

### Base URLs / Regions

- NA: `https://advertising-api.amazon.com/`
- EU: `https://advertising-api-eu.amazon.com/`
- FE: `https://advertising-api-fe.amazon.com/`

### Discoverable endpoint families and connector table mapping

#### Core account/config objects

- `profiles` -> `GET /v2/profiles`
- `portfolios` -> `POST /portfolios/list`

#### Sponsored Products entity objects

- `sponsored_product_campaigns` -> `POST /sp/campaigns/list`
- `sponsored_product_ad_groups` -> `POST /sp/adGroups/list`
- `sponsored_product_keywords` -> `POST /sp/keywords/list`
- `sponsored_product_negative_keywords` -> `POST /sp/negativeKeywords/list`
- `sponsored_product_campaign_negative_keywords` -> `POST /sp/campaignNegativeKeywords/list`
- `sponsored_product_ads` -> `POST /sp/productAds/list`
- `sponsored_product_targetings` -> `POST /sp/targets/list`
- `sponsored_product_ad_group_bid_recommendations` -> `POST /sp/targets/bid/recommendations`
- `sponsored_product_ad_group_suggested_keywords` -> `GET /v2/sp/adGroups/{adGroupId}/suggested/keywords`

#### Sponsored Brands entity objects

- `sponsored_brands_campaigns` -> `POST /sb/v4/campaigns/list`
- `sponsored_brands_ad_groups` -> `POST /sb/v4/adGroups/list`
- `sponsored_brands_keywords` -> `GET /sb/keywords`

#### Sponsored Display entity objects

- `sponsored_display_campaigns` -> `GET /sd/campaigns`
- `sponsored_display_ad_groups` -> `GET /sd/adGroups`
- `sponsored_display_product_ads` -> `GET /sd/productAds`
- `sponsored_display_targetings` -> `GET /sd/targets`
- `sponsored_display_creatives` -> `GET /sd/creatives`
- `sponsored_display_budget_rules` -> budget-rules endpoint family (connector references include `sp/budgetRules`; confirm per account/region during implementation test)

#### Attribution reporting objects

- `attribution_report_products` -> `POST /attribution/report` with product-style payload
- `attribution_report_performance_adgroup`
- `attribution_report_performance_campaign`
- `attribution_report_performance_creative`

Attribution uses request-body parameters (for example `reportType`, advertiser filters, date range, metrics, `cursorId`) rather than one endpoint per report grain.

#### Reporting v3 async objects (fact tables)

Common workflow for report-stream families:

1. `POST /reporting/reports` (create report job)
2. `GET /reporting/reports/{reportId}` (poll status + fetch download URL)
3. download gzip JSON from returned URL

Report families commonly used by connectors:

- Sponsored Products: campaign, ad group, keyword, target, product ad, ASIN report variants
- Sponsored Display: campaign, ad group, target, product ad, ASIN report variants
- Sponsored Brands: v3 purchased-product report variants

Each family is typically implemented in summary and daily variants.

## Object Schema

Amazon Ads does not expose one universal schema endpoint. Schema strategy:

1. Use endpoint response contracts/OpenAPI for entity objects.
2. For report objects, schema is defined by report request config (`adProduct`, `reportTypeId`, `groupBy`, `columns`, `timeUnit`).
3. During connector development, run `discover` against real profiles to lock final field sets and nullability.

Representative field patterns:

- IDs: `profileId`, `campaignId`, `adGroupId`, `adId`, `keywordId`, `targetId`, `portfolioId`
- lifecycle fields: `state`, `servingStatus`, moderation/status fields
- performance fields: `impressions`, `clicks`, `cost`, attributed sales/conversions, units
- date fields: `startDate`, `endDate`, `reportDate` (or equivalent reporting grain field)

## Get Object Primary Keys

Recommended PKs for ingestion:

- `profiles`: `profileId`
- `portfolios`: `portfolioId`
- campaign tables: `campaignId`
- ad group tables: `adGroupId`
- product ad tables: `adId`
- keyword tables: `keywordId`
- targeting tables: `targetId` or endpoint-specific targeting identifier
- report tables: composite PK (for example `profileId + reportDate + entity grain id`)

For recommendation/suggestion streams, use composite logical keys (parent entity + suggestion dimensions), as single immutable IDs are not always present.

## Object's ingestion type

- `snapshot`
  - `profiles`
  - recommendation/suggestion style streams (for example bid recommendations, suggested keywords)
- `cdc`
  - most entity tables (campaign/ad group/ad/keyword/target/portfolio), implemented as periodic upserts by PK
- `append`
  - reporting v3 streams (date-grained fact data)
  - attribution report streams
- `cdc_with_deletes`
  - generally not available as dedicated delete feeds; treat archived/disabled states as soft-delete semantics

## Read API for Data Retrieval

### API patterns

- **Synchronous list/read** for metadata entities (`GET` and `POST .../list`)
- **Asynchronous report generation** for Reporting v3
- **Body-driven report retrieval with cursor** for Attribution

### Pagination

Amazon Ads uses mixed pagination styles:

- offset pagination (`startIndex`, `count`) on many legacy/synchronous list endpoints
- token pagination (`nextToken`, `maxResults`) on v3/v4 list endpoints
- attribution pagination (`cursorId`, `count`) in request body
- async polling for report jobs (`reportId` status endpoint)

### Incremental sync strategy

Recommended strategy by table type:

- **Entity tables**: snapshot + upsert by PK on each sync; optionally partition by profile.
- **Report tables**: incremental by `reportDate` (daily cursor), with configurable lookback window for late attribution and corrections.
- **Attribution reports**: incremental date windows, cursor paging via `cursorId`.

Retention windows are report-family dependent (for example, commonly documented windows include ~60-95 days depending on ad product/report type), so connector logic should clamp requested date ranges to avoid "too far in the past" errors.

### Rate limits

- Amazon Ads uses dynamic throttling (429) with limits varying by endpoint, account, region, and load.
- Public docs do not provide one stable global quota table.

Connector behavior should include:

- exponential backoff with jitter for 429/5xx,
- configurable worker concurrency,
- conservative concurrency for report creation/polling,
- idempotent retries for async report lifecycle calls.

## Field Type Mapping

Recommended normalization:

- `string` -> `string`
- integer-like IDs/counters -> `long` (or `string` for very large identifiers)
- metric numeric values -> `double` / `decimal`
- `boolean` -> `boolean`
- date strings (`YYYY-MM-DD`, `YYYYMMDD`) -> `date`
- datetime strings -> `timestamp`
- nested objects/arrays -> `struct`/`array` where stable, otherwise JSON string

## Recommended Table Scope

Implement these first for high value + stable API behavior:

1. `profiles`
2. `portfolios`
3. `sponsored_product_campaigns`
4. `sponsored_product_ad_groups`
5. `sponsored_product_keywords`
6. `sponsored_product_ads`
7. `sponsored_product_targetings`
8. `sponsored_products_campaigns_report_stream_daily`
9. `sponsored_products_adgroups_report_stream_daily`
10. `sponsored_products_keywords_report_stream_daily`
11. `sponsored_products_productads_report_stream_daily`
12. `sponsored_products_targets_report_stream_daily`
13. `sponsored_display_campaigns_report_stream_daily`
14. `sponsored_brands_v3_report_stream_daily`

Follow-up phase:

- remaining Sponsored Display/Sponsored Brands metadata tables,
- ASIN-grain report tables,
- Attribution report tables,
- recommendation/suggestion tables.

## Sources and References

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official docs hub | https://advertising.amazon.com/API/docs | 2026-03-30 | High | Canonical API docs entry, API families and navigation. |
| Official docs - profiles auth guide | https://advertising.amazon.com/API/docs/en-us/guides/account-management/authorization/profiles | 2026-03-30 | High | Profile-scoped access model and profiles concept. |
| Official docs - limits | https://advertising.amazon.com/API/docs/en-us/concepts/limits | 2026-03-30 | Medium | Platform limits context. |
| Official docs - rate limiting | https://advertising.amazon.com/API/docs/en-us/reference/concepts/rate-limiting | 2026-03-30 | Medium | Dynamic throttling model and 429 handling need. |
| Airbyte Amazon Ads connector docs | https://docs.airbyte.com/integrations/sources/amazon-ads/ | 2026-03-30 | High | Supported streams, region model, practical retention/rate-limit notes. |
| Airbyte connector manifest | https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-integrations/connectors/source-amazon-ads/manifest.yaml | 2026-03-30 | High | Concrete endpoint patterns, PKs, pagination styles, report stream inventory. |
| Fivetran Amazon Ads connector docs | https://fivetran.com/docs/connectors/applications/amazon-ads | 2026-03-30 | Medium | Product families, report retention behavior, operational sync guidance. |
| Fivetran setup guide | https://fivetran.com/docs/connectors/applications/amazon-ads/setup-guide | 2026-03-30 | Medium | Practical role/permission expectations for metadata and report access. |
| Python Amazon Ads API docs | https://python-amazon-ad-api.readthedocs.io/en/latest/ | 2026-03-30 | Medium | Endpoint-family cross-check for SP/SB/SD/Attribution operations and pagination args. |
