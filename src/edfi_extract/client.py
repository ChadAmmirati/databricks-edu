"""
Minimal, production-grade Ed-Fi v3 API client.

Designed as a drop-in replacement for `edfi_api_client.EdFiClient` covering the
slice that EDU's ingestion actually uses:
  - OAuth2 client_credentials with automatic refresh (120s early-refresh buffer)
  - Change-version window stepping (default 50k) so deep offsets never happen
  - Reverse-offset paging inside each window: pages are walked from the end
    toward 0 so rows shifting between pages mid-pull cause duplicates (handled
    by Silver dedupe) instead of silent misses
  - /deletes and /keyChanges endpoints
  - Retries with exponential backoff on transient failures; 401 triggers
    token refresh and retry
  - Typed EdFiNotFound on 404 so callers can skip version-gated endpoints

No Airflow or Snowflake dependencies. Pure Python + requests so it runs the
same on a Databricks cluster, on serverless, or in a unit test.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Iterator, List, Optional

import requests

logger = logging.getLogger(__name__)

_DEFAULT_PAGE_SIZE = 500
_DEFAULT_MAX_RETRIES = 5
_DEFAULT_TIMEOUT_S = 60
_DEFAULT_CV_STEP = 50_000
_RETRY_STATUS = {401, 408, 429, 500, 502, 503, 504}


class EdFiNotFound(Exception):
    """Endpoint does not exist on this ODS (version-gated or disabled)."""


@dataclass
class TokenState:
    access_token: str
    expires_at: float  # epoch seconds


class EdFiClient:
    def __init__(
        self,
        base_url: str,
        client_key: str,
        client_secret: str,
        *,
        page_size: int = _DEFAULT_PAGE_SIZE,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        timeout_s: int = _DEFAULT_TIMEOUT_S,
        change_version_step: int = _DEFAULT_CV_STEP,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.client_key = client_key
        self.client_secret = client_secret
        self.page_size = page_size
        self.max_retries = max_retries
        self.timeout_s = timeout_s
        self.change_version_step = change_version_step
        self._session = session or requests.Session()
        self._token: Optional[TokenState] = None

    # ---------- auth ----------
    def _refresh_token(self) -> None:
        url = f"{self.base_url}/oauth/token"
        resp = self._session.post(
            url,
            data={"grant_type": "client_credentials"},
            auth=(self.client_key, self.client_secret),
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        body = resp.json()
        # Refresh 120s before expiry so a token never goes stale mid-pull.
        self._token = TokenState(
            access_token=body["access_token"],
            expires_at=time.time() + float(body.get("expires_in", 1800)) - 120,
        )
        logger.info("Refreshed Ed-Fi OAuth token (expires in %ss).", body.get("expires_in"))

    def _auth_header(self) -> dict:
        if self._token is None or time.time() >= self._token.expires_at:
            self._refresh_token()
        return {"Authorization": f"Bearer {self._token.access_token}"}

    # ---------- request layer ----------
    def _get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self._session.get(
                    url,
                    params=params or {},
                    headers=self._auth_header(),
                    timeout=self.timeout_s,
                )
            except requests.RequestException as exc:
                if attempt > self.max_retries:
                    raise
                _sleep_backoff(attempt, reason=str(exc))
                continue

            if resp.status_code == 404:
                raise EdFiNotFound(url)
            if resp.status_code in _RETRY_STATUS and attempt <= self.max_retries:
                if resp.status_code == 401:
                    self._token = None  # revoked/expired mid-flight: re-auth on retry
                _sleep_backoff(attempt, reason=f"HTTP {resp.status_code}")
                continue
            resp.raise_for_status()
            return resp

    def _resource_url(self, path: str, suffix: str = "") -> str:
        return f"{self.base_url}/data/v3/{path.lstrip('/')}{suffix}"

    # ---------- public API ----------
    def get_latest_change_version(self) -> int:
        """Newest available change version. Handles the Ed-Fi 6.0 key-casing change."""
        url = f"{self.base_url}/changeQueries/v1/availableChangeVersions"
        resp = self._get(url)
        body = {k.lower(): v for k, v in resp.json().items()}
        return int(body.get("newestchangeversion") or 0)

    def get_total_count(
        self,
        path: str,
        *,
        suffix: str = "",
        min_change_version: Optional[int] = None,
        max_change_version: Optional[int] = None,
    ) -> Optional[int]:
        """Total-Count for a resource (optionally within a CV window). None if unsupported."""
        params = {"limit": 0, "totalCount": "true"}
        if min_change_version is not None:
            params["minChangeVersion"] = min_change_version
        if max_change_version is not None:
            params["maxChangeVersion"] = max_change_version
        resp = self._get(self._resource_url(path, suffix), params=params)
        total = resp.headers.get("Total-Count")
        return int(total) if total is not None else None

    def get_resource(
        self,
        path: str,
        *,
        min_change_version: Optional[int] = None,
        max_change_version: Optional[int] = None,
        suffix: str = "",
    ) -> Iterator[List[dict]]:
        """
        Yields pages (lists) of records from the given resource path.

        When a change-version window is supplied, the window is walked in
        `change_version_step` sub-windows and each sub-window is paged in
        REVERSE offset order (mirrors edfi_api_client): rows shifting between
        pages during the pull then produce duplicate rows — which Silver's
        dedupe absorbs — instead of silently missed rows.
        """
        if min_change_version is None and max_change_version is None:
            yield from self._page_window(path, suffix, None, None)
            return

        lo = min_change_version or 0
        hi = max_change_version if max_change_version is not None else lo + self.change_version_step
        while lo <= hi:
            window_hi = min(lo + self.change_version_step - 1, hi)
            yield from self._page_window(path, suffix, lo, window_hi)
            lo = window_hi + 1

    def get_deletes(
        self,
        path: str,
        *,
        min_change_version: Optional[int] = None,
        max_change_version: Optional[int] = None,
    ) -> Iterator[List[dict]]:
        """Pages of {id, keyValues, changeVersion} tombstones from /deletes."""
        yield from self.get_resource(
            path,
            min_change_version=min_change_version,
            max_change_version=max_change_version,
            suffix="/deletes",
        )

    def get_key_changes(
        self,
        path: str,
        *,
        min_change_version: Optional[int] = None,
        max_change_version: Optional[int] = None,
    ) -> Iterator[List[dict]]:
        yield from self.get_resource(
            path,
            min_change_version=min_change_version,
            max_change_version=max_change_version,
            suffix="/keyChanges",
        )

    # ---------- paging ----------
    def _page_window(
        self,
        path: str,
        suffix: str,
        min_cv: Optional[int],
        max_cv: Optional[int],
    ) -> Iterator[List[dict]]:
        url = self._resource_url(path, suffix)

        def cv_params() -> dict:
            p = {}
            if min_cv is not None:
                p["minChangeVersion"] = min_cv
            if max_cv is not None:
                p["maxChangeVersion"] = max_cv
            return p

        total: Optional[int] = None
        if min_cv is not None or max_cv is not None:
            try:
                total = self.get_total_count(
                    path, suffix=suffix,
                    min_change_version=min_cv, max_change_version=max_cv,
                )
            except EdFiNotFound:
                raise
            except Exception:
                logger.warning("Total-Count unavailable for %s; falling back to forward paging.", path)

        if total is not None:
            if total == 0:
                return
            # Reverse paging: start at the last full page, walk down to 0.
            offset = (max(total - 1, 0) // self.page_size) * self.page_size
            while offset >= 0:
                params = {"offset": offset, "limit": self.page_size, **cv_params()}
                page = self._get(url, params=params).json()
                if page:
                    yield page
                offset -= self.page_size
            return

        # Forward paging fallback (no CV window, or Total-Count unsupported).
        offset = 0
        while True:
            params = {"offset": offset, "limit": self.page_size, **cv_params()}
            page = self._get(url, params=params).json()
            if not page:
                return
            yield page
            if len(page) < self.page_size:
                return
            offset += self.page_size


def _sleep_backoff(attempt: int, *, reason: str) -> None:
    delay = min(60.0, 2 ** attempt)
    logger.warning("Retrying after %.1fs (attempt %d): %s", delay, attempt, reason)
    time.sleep(delay)
