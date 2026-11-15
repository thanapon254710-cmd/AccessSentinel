"""Decoding and identity of stream records. Validation failures raise DecodeError (-> dead-letter queue);
error messages never echo payload contents."""
import json
from typing import Any

from pydantic import ValidationError

from ..events import AccessEvent
from ..schema import AccessEventV1
from .consumer import DecodeError, Record


def decode_access_event(value: bytes) -> AccessEvent:
    return AccessEventV1(**decode_access_dict(value)).to_event()


def decode_access_dict(value: bytes) -> dict:
    """Validate an access event (schema v1 or v2) and return it as a plain dict (used by the v3 stages)."""
    try:
        obj = json.loads(value)
    except (ValueError, TypeError):
        raise DecodeError("payload is not valid JSON") from None
    if not isinstance(obj, dict):
        raise DecodeError("payload is not a JSON object")
    try:
        AccessEventV1(**obj)
        return obj
    except ValidationError as e:
        fields = sorted({".".join(str(x) for x in err["loc"]) for err in e.errors()})[:5]
        raise DecodeError(f"schema violation in fields {fields}") from None
    except TypeError:
        raise DecodeError("schema violation") from None


def decode_json_object(value: bytes) -> dict:
    try:
        obj = json.loads(value)
    except (ValueError, TypeError):
        raise DecodeError("payload is not valid JSON") from None
    if not isinstance(obj, dict):
        raise DecodeError("payload is not a JSON object")
    return obj


def record_id(rec: Record) -> str:
    """Stable identity used for deduplication. Access events: event_id (set once by the gateway).
    Other records: their own id field if present, else the Kafka coordinates (stable across redelivery)."""
    d: Any = rec.data
    if isinstance(d, AccessEvent):
        return d.event_id
    if isinstance(d, dict):
        # a record's OWN id first: e.g. a risk score also carries the event_id of the event it scores
        for k in ("record_id", "score_id", "escalation_id", "decision_id", "feedback_id", "event_id"):
            if isinstance(d.get(k), str) and d[k]:
                return d[k]
    return f"{rec.topic}:{rec.partition}:{rec.offset}"
