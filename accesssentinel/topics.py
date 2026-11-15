"""Single registry of Kafka topics (see docs/PHASE1_AUDIT.md §5.3). Used by topic provisioning,
producers, consumers and the infrastructure check so names are defined in exactly one place."""
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(frozen=True)
class TopicSpec:
    name: str
    key: str            # what the message key is (documentation + checks)
    purpose: str
    dlq: bool = True    # consumers of this topic route poison messages to `<name>.dlq`
    config: Dict[str, str] = field(default_factory=dict)   # topic-level Kafka config enforced by provisioning


ACCESS_EVENTS = "access-events"
ACCESS_EVENTS_BY_RESOURCE = "access-events-by-resource"
RISK_SCORES = "risk-scores"
ESCALATIONS_REALTIME = "escalations.realtime"
ESCALATIONS_BACKGROUND = "escalations.background"
GROUP_SCORES = "group-scores"
PDP_DECISIONS = "pdp-decisions"
ANALYST_FEEDBACK = "analyst-feedback"
# ---- Revised v3 event pipeline (Phase 2)
ACCESS_EVENTS_ORDERED = "access-events.ordered"
LATE_EVENTS = "late-events"
SEQUENCER_CHANGELOG = "sequencer.changelog"
RESOURCE_PARTIALS = "resource-partials"
RESOURCE_WINDOWS = "resource-windows"
RES_SHARD_CHANGELOG = "resource-shards.changelog"
RES_MERGE_CHANGELOG = "resource-merge.changelog"

_COMPACT = {"cleanup.policy": "compact", "min.cleanable.dirty.ratio": "0.1", "segment.ms": "600000"}

TOPICS: List[TopicSpec] = [
    TopicSpec(ACCESS_EVENTS, "user id u", "Step 2: gateway access events (append order per user); broker time = ti",
              config={"message.timestamp.type": "LogAppendTime"}),
    TopicSpec(ACCESS_EVENTS_BY_RESOURCE, "r*|b|shard (v3 Stage 1)", "Step 2: resource-keyed view"),
    TopicSpec(ACCESS_EVENTS_ORDERED, "user id u", "v3 Step 2: per-user EVENT-TIME order (watermark, Eq. 2) -> TCN, CMU"),
    TopicSpec(LATE_EVENTS, "user id u", "v3 Step 2: late-event alerts and discard audit records", dlq=False),
    TopicSpec(SEQUENCER_CHANGELOG, "user id u", "checkpointed ordering state (compacted)", dlq=False, config=_COMPACT),
    TopicSpec(RESOURCE_PARTIALS, "r*|b", "v3 Step 2 Stage 1: partial distinct-user sets per shard"),
    TopicSpec(RESOURCE_WINDOWS, "r*|b", "v3 Step 2 Stage 2: merged, finalised resource windows (exactly once)"),
    TopicSpec(RES_SHARD_CHANGELOG, "r*|b|shard", "checkpointed Stage-1 state (compacted)", dlq=False, config=_COMPACT),
    TopicSpec(RES_MERGE_CHANGELOG, "r*|b", "checkpointed Stage-2 state (compacted)", dlq=False, config=_COMPACT),
    TopicSpec(RISK_SCORES, "user id u", "D3 Steps 4-5: TCN individual risk S_ind"),
    TopicSpec(ESCALATIONS_REALTIME, "user id u", "D3 Step 6: paths A (high) and B (resource)"),
    TopicSpec(ESCALATIONS_BACKGROUND, "user id u", "D3 Step 6: paths C (sweep) and D (sampling)"),
    TopicSpec(GROUP_SCORES, "group id", "D3 Step 7: TGN collective risk S_coll"),
    TopicSpec(PDP_DECISIONS, "user id u", "D3 Step 8: Allow/Block decisions"),
    TopicSpec(ANALYST_FEEDBACK, "incident id", "D3 Step 9: analyst-verified outcomes"),
]


def topic_name(name: str, prefix: str = "") -> str:
    return f"{prefix}.{name}" if prefix else name


def topic_configs(prefix: str = "") -> Dict[str, Dict[str, str]]:
    return {topic_name(t.name, prefix): dict(t.config) for t in TOPICS if t.config}


def all_topic_names(prefix: str = "", include_dlq: bool = True) -> List[str]:
    out = []
    for t in TOPICS:
        out.append(topic_name(t.name, prefix))
        if include_dlq and t.dlq:
            out.append(topic_name(f"{t.name}.dlq", prefix))
    return out
