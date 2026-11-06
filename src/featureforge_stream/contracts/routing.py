"""Separate valid event JSON from structured validation failures."""

import json

from pydantic import ValidationError

from featureforge_stream.contracts.events import validate_event_json


def route_event_json(payload: str) -> tuple[str | None, str | None]:
    """Return exactly one populated result: valid JSON or rejection JSON.

    Catch input-validation failures only. Unexpected programming or runtime
    errors remain visible and are not converted into rejected records.
    """
    try:
        return validate_event_json(payload), None
    except ValidationError as exc:
        errors = exc.errors(
            include_url=False,
            include_context=False,
            include_input=False,
        )
        reason_code = (
            "INVALID_JSON"
            if any(error["type"] == "json_invalid" for error in errors)
            else "EVENT_CONTRACT_VIOLATION"
        )

        rejection = {
            "error_schema_version": "v1",
            "reason_code": reason_code,
            "raw_payload": payload,
            "errors": errors,
        }

        return None, json.dumps(rejection, sort_keys=True)
