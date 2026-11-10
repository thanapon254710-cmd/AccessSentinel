"""Decoding and identity of stream records. Validation failures raise DecodeError (-> dead-letter queue);
error messages never echo payload contents."""
import json
from typing import Any

from pydantic import ValidationError

from ..events import AccessEvent
from ..schema import AccessEventV1
from .consumer import DecodeError, Record


def decode_access_event(value: bytes) -> AccessEvent:
    try:
        obj = json.loads(value)
    except (ValueError, TypeError):
        raise DecodeError("payload is not valid JSON") from None
    if not isinstance(obj, dict):
        raise DecodeError("payload is not a JSON object")
    try:
        return AccessEventV1(**obj).to_event()
    except ValidationError as e:
        fields = sorted({".".join(str(x) for x in err["loc"]) for err in e.errors()})[:5]
        raise DecodeError(f"schema v1 violation in fields {fields}") from None
    except TypeError:
        raise DecodeError("schema v1 violation") from None


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
        for k in ("record_id", "event_id", "decision_id", "feedback_id", "score_id"):
            if isinstance(d.get(k), str) and d[k]:
                return d[k]
    return f"{rec.topic}:{rec.partition}:{rec.offset}"
