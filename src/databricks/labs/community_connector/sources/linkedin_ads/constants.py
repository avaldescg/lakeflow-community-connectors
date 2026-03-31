"""Constants for the LinkedIn Ads connector."""

LINKEDIN_REST_BASE_URL = "https://api.linkedin.com/rest"

# Linkedin Marketing APIs require a YYYYMM version header. Use an overridable default.
DEFAULT_LINKEDIN_VERSION = "202603"

# Common retriable statuses (rate limit + transient server errors).
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}

DEFAULT_INITIAL_BACKOFF_SECONDS = 1.0
DEFAULT_MAX_RETRIES = 8

# LinkedIn commonly supports start/count pagination with count caps varying by endpoint.
DEFAULT_PAGE_SIZE = 100
