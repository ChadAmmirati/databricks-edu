"""Unit tests for the Ed-Fi API client. No live API calls — all requests are mocked."""
from __future__ import annotations

from unittest.mock import patch

import pytest
import requests

from edfi_extract.client import EdFiClient, EdFiNotFound


class _FakeResponse:
    def __init__(self, status_code: int, body, headers=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            raise requests.HTTPError(f"{self.status_code}", response=self)


def _client(responses, **kwargs):
    """Helper: build a client whose session returns canned responses in order."""
    iter_responses = iter(responses)

    class _Session:
        def post(self, *a, **kw):
            return next(iter_responses)
        def get(self, *a, **kw):
            return next(iter_responses)

    return EdFiClient(
        base_url="https://api.example/v7.3/api",
        client_key="k", client_secret="s",
        session=_Session(),
        max_retries=2,
        **kwargs,
    )


_TOKEN = _FakeResponse(200, {"access_token": "tok", "expires_in": 1800})


def test_refresh_token_sets_state():
    c = _client([_FakeResponse(200, {"access_token": "tok-1", "expires_in": 1800})])
    assert c._auth_header() == {"Authorization": "Bearer tok-1"}
    # second call uses cached token, no extra request needed (no responses left)
    assert c._auth_header() == {"Authorization": "Bearer tok-1"}


def test_get_resource_pages_until_empty():
    page1 = [{"id": "a"}, {"id": "b"}]
    c = _client([_TOKEN, _FakeResponse(200, page1), _FakeResponse(200, [])])
    c.page_size = 2
    pages = list(c.get_resource("ed-fi/students"))
    assert pages == [page1]


def test_get_resource_short_page_terminates():
    c = _client([_TOKEN, _FakeResponse(200, [{"id": "a"}])])
    pages = list(c.get_resource("ed-fi/students"))
    assert pages == [[{"id": "a"}]]


def test_retries_on_503_then_succeeds():
    c = _client([_TOKEN, _FakeResponse(503, {}), _FakeResponse(200, [{"id": "x"}])])
    with patch("edfi_extract.client.time.sleep"):
        pages = list(c.get_resource("ed-fi/students"))
    assert pages == [[{"id": "x"}]]


def test_token_refresh_on_401():
    c = _client([
        _FakeResponse(200, {"access_token": "tok-1", "expires_in": 1800}),
        _FakeResponse(401, {}),
        _FakeResponse(200, {"access_token": "tok-2", "expires_in": 1800}),
        _FakeResponse(200, [{"id": "y"}]),
    ])
    with patch("edfi_extract.client.time.sleep"):
        pages = list(c.get_resource("ed-fi/students"))
    assert pages == [[{"id": "y"}]]


def test_404_raises_typed_not_found():
    c = _client([_TOKEN, _FakeResponse(404, {})])
    with pytest.raises(EdFiNotFound):
        list(c.get_resource("ed-fi/parents"))


def test_get_latest_change_version_handles_both_casings():
    c = _client([_TOKEN, _FakeResponse(200, {"newestChangeVersion": 12345})])
    assert c.get_latest_change_version() == 12345
    c = _client([_TOKEN, _FakeResponse(200, {"NewestChangeVersion": 99})])
    assert c.get_latest_change_version() == 99


def test_cv_window_uses_reverse_paging():
    """Within a CV window, paging starts from the LAST page and walks to 0."""
    seen_offsets = []

    class _Session:
        def post(self, *a, **kw):
            return _FakeResponse(200, {"access_token": "tok", "expires_in": 1800})
        def get(self, url, params=None, **kw):
            params = params or {}
            if params.get("limit") == 0:
                return _FakeResponse(200, [], headers={"Total-Count": "5"})
            seen_offsets.append(params["offset"])
            return _FakeResponse(200, [{"id": f"r{params['offset']}"}])

    c = EdFiClient(base_url="https://api.example/v7.3/api", client_key="k",
                   client_secret="s", session=_Session(), page_size=2)
    rows = [r for page in c.get_resource("ed-fi/students", min_change_version=1, max_change_version=100) for r in page]
    assert seen_offsets == [4, 2, 0]
    assert len(rows) == 3


def test_cv_window_stepping_splits_large_ranges():
    """A CV range wider than change_version_step is pulled in sub-windows."""
    windows = []

    class _Session:
        def post(self, *a, **kw):
            return _FakeResponse(200, {"access_token": "tok", "expires_in": 1800})
        def get(self, url, params=None, **kw):
            params = params or {}
            if params.get("limit") == 0:
                windows.append((params["minChangeVersion"], params["maxChangeVersion"]))
                return _FakeResponse(200, [], headers={"Total-Count": "0"})
            raise AssertionError("empty windows should not be paged")

    c = EdFiClient(base_url="https://api.example/v7.3/api", client_key="k",
                   client_secret="s", session=_Session(), change_version_step=10)
    list(c.get_resource("ed-fi/students", min_change_version=1, max_change_version=25))
    assert windows == [(1, 10), (11, 20), (21, 25)]


def test_deletes_uses_suffix():
    urls = []

    class _Session:
        def post(self, *a, **kw):
            return _FakeResponse(200, {"access_token": "tok", "expires_in": 1800})
        def get(self, url, params=None, **kw):
            urls.append(url)
            if (params or {}).get("limit") == 0:
                return _FakeResponse(200, [], headers={"Total-Count": "1"})
            return _FakeResponse(200, [{"id": "gone", "changeVersion": 7}])

    c = EdFiClient(base_url="https://api.example/v7.3/api", client_key="k",
                   client_secret="s", session=_Session())
    pages = list(c.get_deletes("ed-fi/students", min_change_version=1, max_change_version=5))
    assert pages == [[{"id": "gone", "changeVersion": 7}]]
    assert all(u.endswith("/students/deletes") for u in urls)
