"""Tests for the shared backoff helpers."""

from __future__ import annotations

import pytest

from nocoly_explorer.backoff import compute_backoff, parse_retry_after


class TestComputeBackoff:
    def test_grows_exponentially_without_jitter(self):
        delays = [
            compute_backoff(
                attempt=a, base_delay=0.5, max_delay=30.0,
                max_wait_seconds=30.0, jitter_factor=0.0,
            )
            for a in range(5)
        ]
        assert delays == [0.5, 1.0, 2.0, 4.0, 8.0]

    def test_caps_at_max_delay(self):
        assert compute_backoff(
            attempt=20, base_delay=0.5, max_delay=10.0,
            max_wait_seconds=30.0, jitter_factor=0.0,
        ) == 10.0

    def test_caps_at_max_wait_seconds(self):
        assert compute_backoff(
            attempt=20, base_delay=0.5, max_delay=8.0,
            max_wait_seconds=30.0, jitter_factor=0.0,
        ) == 8.0

    def test_jitter_within_factor(self):
        for _ in range(50):
            delay = compute_backoff(
                attempt=2, base_delay=0.5, max_delay=30.0,
                max_wait_seconds=30.0, jitter_factor=0.5,
            )
            assert 2.0 <= delay <= 3.0

    def test_rejects_invalid_args(self):
        with pytest.raises(ValueError):
            compute_backoff(attempt=-1, base_delay=0.5, max_delay=30.0, max_wait_seconds=30.0)
        with pytest.raises(ValueError):
            compute_backoff(attempt=0, base_delay=0.0, max_delay=30.0, max_wait_seconds=30.0)
        with pytest.raises(ValueError):
            compute_backoff(attempt=0, base_delay=0.5, max_delay=30.0, max_wait_seconds=0.0)
        with pytest.raises(ValueError):
            compute_backoff(attempt=0, base_delay=0.5, max_delay=30.0, max_wait_seconds=30.0, jitter_factor=1.5)


class TestParseRetryAfter:
    def test_delta_seconds(self):
        assert parse_retry_after("5", max_wait_seconds=30.0) == 5.0

    def test_delta_seconds_capped(self):
        assert parse_retry_after("1000", max_wait_seconds=30.0) == 30.0

    def test_http_date_far_future_caps(self):
        assert parse_retry_after("Wed, 21 Oct 2099 07:28:00 GMT", max_wait_seconds=30.0) == 30.0

    def test_http_date_past_returns_zero(self):
        assert parse_retry_after("Wed, 21 Oct 1990 07:28:00 GMT", max_wait_seconds=30.0) == 0.0

    def test_invalid_returns_none(self):
        assert parse_retry_after("not-a-date", max_wait_seconds=30.0) is None
        assert parse_retry_after("", max_wait_seconds=30.0) is None
        assert parse_retry_after(None, max_wait_seconds=30.0) is None
        assert parse_retry_after("   ", max_wait_seconds=30.0) is None

    def test_negative_seconds_rejected(self):
        assert parse_retry_after("-1", max_wait_seconds=30.0) is None
