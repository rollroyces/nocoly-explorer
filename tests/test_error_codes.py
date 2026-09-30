"""Tests for the stable error codes on every exception class."""

from __future__ import annotations

import pytest

from nocoly_explorer.exceptions import (
    AsyncClientError,
    CardinalityExceededError,
    EnvironmentDetectionError,
    JobCancelled,
    JobNotFound,
    JobNotReady,
    MissingCredentialsError,
    NocolyError,
    OutputValidationError,
    PaginationLimitExceeded,
    SchemaDriftError,
    ServiceError,
)


# (class, code) pairs that should be stable across releases.
EXPECTED_CODES = {
    NocolyError: "nocoly_error",
    MissingCredentialsError: "missing_credentials",
    OutputValidationError: "output_validation",
    EnvironmentDetectionError: "environment_detection",
    SchemaDriftError: "schema_drift",
    CardinalityExceededError: "cardinality_exceeded",
    AsyncClientError: "async_client_error",
    PaginationLimitExceeded: "pagination_limit_exceeded",
    ServiceError: "service_error",
    JobNotFound: "job_not_found",
    JobNotReady: "job_not_ready",
    JobCancelled: "job_cancelled",
}


class TestCodesAssigned:
    @pytest.mark.parametrize("cls,code", list(EXPECTED_CODES.items()))
    def test_class_has_expected_code(self, cls, code):
        assert cls.code == code

    @pytest.mark.parametrize("cls,code", list(EXPECTED_CODES.items()))
    def test_instance_carries_code(self, cls, code):
        # Some exceptions take no args; some take a message. Both should
        # expose .code on the instance.
        try:
            inst = cls("test")
        except TypeError:
            inst = cls()
        assert inst.code == code


class TestCodesAreUnique:
    def test_no_two_classes_share_a_code(self):
        codes = [code for _, code in EXPECTED_CODES.items()]
        # Base class codes may be reused by subclasses; but otherwise
        # leaf codes should be unique. Just check no two LEAF classes
        # share a code.
        leaf_codes = [
            (cls.__name__, code) for cls, code in EXPECTED_CODES.items()
            if cls not in (NocolyError, AsyncClientError, ServiceError)
        ]
        seen = set()
        for name, code in leaf_codes:
            assert code not in seen, f"{name} reuses code {code!r}"
            seen.add(code)


class TestInheritance:
    def test_subclasses_inherit_nocoly_error(self):
        for cls in EXPECTED_CODES:
            assert issubclass(cls, NocolyError), cls

    def test_code_round_trip_through_raise(self):
        with pytest.raises(MissingCredentialsError) as exc:
            raise MissingCredentialsError("missing app_key")
        assert exc.value.code == "missing_credentials"
        assert "missing app_key" in str(exc.value)


class TestStableCodes:
    """These codes are part of the public contract. Changing them is a
    breaking change. If you intentionally need to change one, update
    this test and the README's Error codes table in the same commit.
    """

    @pytest.mark.parametrize("code", [
        "missing_credentials", "output_validation", "environment_detection", "schema_drift",
        "cardinality_exceeded", "async_client_error", "pagination_limit_exceeded", "service_error",
        "job_not_found", "job_not_ready", "job_cancelled",
    ])
    def test_code_still_assigned(self, code):
        # Reverse-lookup: every documented code maps to a class.
        all_codes = {cls.code: cls for cls in EXPECTED_CODES}
        assert code in all_codes, f"code {code!r} has no matching class"
        assert issubclass(all_codes[code], NocolyError)
