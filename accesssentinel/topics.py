"""Single registry of Kafka topics (see docs/PHASE1_AUDIT.md §5.3). Used by topic provisioning,
producers, consumers and the infrastructure check so names are defined in exactly one place."""
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class TopicSpec:
    name: str
    key: str            # what the message key is (documentation + checks)
    purpose: str
    dlq: bool = True    # consumers of this topic route poison messages to `<name>.dlq`


ACCESS_EVENTS = "access-events"
ACCESS_EVENTS_BY_RESOURCE = "access-events-by-resource"
RISK_SCORES = "risk-scores"
ESCALATIONS_REALTIME = "escalations.realtime"
ESCALATIONS_BACKGROUND = "escalations.background"
GROUP_SCORES = "group-scores"
PDP_DECISIONS = "pdp-decisions"
ANALYST_FEEDBACK = "analyst-feedback"

TOPICS: List[TopicSpec] = [
    TopicSpec(ACCESS_EVENTS, "user id u", "D3 Step 2: gateway access events, per-user order"),
    TopicSpec(ACCESS_EVENTS_BY_RESOURCE, "app:resource#time-bucket", "D3 Step 2: resource-keyed view"),
    TopicSpec(RISK_SCORES, "user id u", "D3 Steps 4-5: TCN individual risk S_ind"),
    TopicSpec(ESCALATIONS_REALTIME, "user id u", "D3 Step 6: paths A (high) and B (resource)"),
    TopicSpec(ESCALATIONS_BACKGROUND, "user id u", "D3 Step 6: paths C (sweep) and D (sampling)"),
    TopicSpec(GROUP_SCORES, "group id", "D3 Step 7: TGN collective risk S_coll"),
    TopicSpec(PDP_DECISIONS, "user id u", "D3 Step 8: Allow/Block decisions"),
    TopicSpec(ANALYST_FEEDBACK, "incident id", "D3 Step 9: analyst-verified outcomes"),
]


def topic_name(name: str, prefix: str = "") -> str:
    return f"{prefix}.{name}" if prefix else name


def all_topic_names(prefix: str = "", include_dlq: bool = True) -> List[str]:
    out = []
    for t in TOPICS:
        out.append(topic_name(t.name, prefix))
        if include_dlq and t.dlq:
            out.append(topic_name(f"{t.name}.dlq", prefix))
    return out
