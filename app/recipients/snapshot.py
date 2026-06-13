"""The per-tenant routing snapshot — what the subscription cache stores (03 §7).

A snapshot is everything the dispatcher needs to route one tenant's alerts with
*zero* further DB calls: the tenant's live subscriptions plus a lookup of every
channel any of them route to, with secret-ref ``config`` resolved at send time
(never the raw secret). It is JSON-serialisable so it can live in Redis and be
rebuilt cheaply on a cache miss.

UUIDs are stored as strings throughout so the structure round-trips through JSON
unchanged; :func:`collect_targets` re-hydrates ``recipient_id`` to a UUID when it
builds :class:`ResolvedTarget`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from uuid import UUID


@dataclass(frozen=True)
class SnapChannel:
    recipient_id: UUID
    kind: str
    address: str
    config: dict = field(default_factory=dict)


@dataclass(frozen=True)
class SnapSubscription:
    topic_pattern: str
    min_severity: str
    channel_ids: list[str]


@dataclass(frozen=True)
class TenantSnapshot:
    subscriptions: list[SnapSubscription]
    channels: dict[str, SnapChannel]  # keyed by channel id (str)

    def to_json(self) -> str:
        return json.dumps(
            {
                "subscriptions": [
                    {
                        "topic_pattern": s.topic_pattern,
                        "min_severity": s.min_severity,
                        "channel_ids": s.channel_ids,
                    }
                    for s in self.subscriptions
                ],
                "channels": {
                    cid: {
                        "recipient_id": str(c.recipient_id),
                        "kind": c.kind,
                        "address": c.address,
                        "config": c.config,
                    }
                    for cid, c in self.channels.items()
                },
            },
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, raw: str) -> TenantSnapshot:
        data = json.loads(raw)
        return cls(
            subscriptions=[
                SnapSubscription(
                    topic_pattern=s["topic_pattern"],
                    min_severity=s["min_severity"],
                    channel_ids=list(s["channel_ids"]),
                )
                for s in data["subscriptions"]
            ],
            channels={
                cid: SnapChannel(
                    recipient_id=UUID(c["recipient_id"]),
                    kind=c["kind"],
                    address=c["address"],
                    config=c.get("config", {}),
                )
                for cid, c in data["channels"].items()
            },
        )
