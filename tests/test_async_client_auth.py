"""Tests for AsyncWorksheetClient's HAP auth header construction.

Regression tests for the bug where auth_token was being sent as a Bearer
token. Nocoly uses HAP-AppKey + HAP-Sign headers, not Bearer.
"""

from __future__ import annotations

import pytest

from nocoly_explorer.async_client import AsyncWorksheetClient


class TestHapHeaders:
    def test_app_key_app_sign_form(self):
        c = AsyncWorksheetClient(
            base_url="https://example.com",
            auth_token="mykey:mysecret",
            worksheet_id="ws_1",
        )
        h = c._hap_headers()
        assert h["HAP-AppKey"] == "mykey"
        assert h["HAP-Sign"] == "mysecret"
        assert h["Content-Type"] == "application/json"
        # Critically, no Authorization: Bearer header.
        assert "Authorization" not in h

    def test_single_token_form(self):
        c = AsyncWorksheetClient(
            base_url="https://example.com",
            auth_token="only_a_token",
            worksheet_id="ws_1",
        )
        h = c._hap_headers()
        # Self-hosted Nocoly instances may use a single token as either
        # header. Send it as both.
        assert h["HAP-AppKey"] == "only_a_token"
        assert h["HAP-Sign"] == "only_a_token"

    def test_token_with_multiple_colons(self):
        # Only the first colon is the separator; the rest stays in the sign.
        c = AsyncWorksheetClient(
            base_url="https://example.com",
            auth_token="key:sign:extra",
            worksheet_id="ws_1",
        )
        h = c._hap_headers()
        assert h["HAP-AppKey"] == "key"
        assert h["HAP-Sign"] == "sign:extra"

    def test_empty_token(self):
        c = AsyncWorksheetClient(
            base_url="https://example.com",
            auth_token="",
            worksheet_id="ws_1",
        )
        h = c._hap_headers()
        # Both headers present but empty. (Callers should never pass an
        # empty token - JobSubmission.auth_token is min_length=1.)
        assert h["HAP-AppKey"] == ""
        assert h["HAP-Sign"] == ""


class TestNoBearerHeader:
    """Regression: the original bug sent `Authorization: Bearer <token>`."""

    def test_bearer_header_never_set(self):
        c = AsyncWorksheetClient(
            base_url="https://example.com",
            auth_token="mykey:mysecret",
            worksheet_id="ws_1",
        )
        h = c._hap_headers()
        assert "Authorization" not in h
        assert "Bearer" not in str(h)



@pytest.mark.asyncio
async def test_session_uses_hap_headers_at_enter():
    """Verify __aenter__ wires _hap_headers into the aiohttp session."""
    from nocoly_explorer.async_client import AsyncWorksheetClient

    c = AsyncWorksheetClient(
        base_url="https://example.com",
        auth_token="mykey:mysecret",
        worksheet_id="ws_1",
    )
    assert c._session is None  # lazy

    async with c:
        # __aenter__ created the session with HAP headers.
        assert c._session is not None
        sent = dict(c._session.headers)
        assert sent["HAP-AppKey"] == "mykey"
        assert sent["HAP-Sign"] == "mysecret"
        assert "Authorization" not in sent
