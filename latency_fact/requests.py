"""离线请求观测。

规则：

* ``request_key`` 在 shard 内标识同一请求；源端重传或日终补报携带相同键时，
  以到达更晚的观测为准，绝不重复累加；
* 属于 ``complete`` 交付的请求是全量事实，参与窗口权威总量；
* 属于 ``sample`` 交付的请求只能证明“存在这样一次请求”，用于抬高慢请求
  下界，绝不补作全量、不进费率分母的权威口径；
* 当采样键后来被同 shard 的全量包覆盖时，采样行降级为冗余印证（去重时记录
  取代关系，证据链仍可追查）。
"""
from __future__ import annotations

from dataclasses import dataclass

from .intake import SourceBundle
from .model import RequestObservation


@dataclass(frozen=True)
class RequestModel:
    version: int
    observations: tuple[RequestObservation, ...]
    # 被更晚全量观测取代的 (record_id -> 取代 record_id)
    superseded_records: dict[str, str]

    def in_span(self, shard: str, span: tuple[int, int]) -> list[RequestObservation]:
        lo, hi = span
        return [
            o
            for o in self.observations
            if o.shard == shard and lo <= o.time < hi
        ]

    def complete_in_span(
        self, shard: str, span: tuple[int, int]
    ) -> list[RequestObservation]:
        return [
            o
            for o in self.in_span(shard, span)
            if o.status == "complete" and o.record_id not in self.superseded_records
        ]

    def samples_in_span(
        self, shard: str, span: tuple[int, int]
    ) -> list[RequestObservation]:
        return [
            o
            for o in self.in_span(shard, span)
            if o.status == "sample" and o.record_id not in self.superseded_records
        ]


def build_requests(bundle: SourceBundle) -> RequestModel:
    delivery_status = {
        row["id"]: row["status"] for row in bundle.table("offline")
    }
    intake_of_delivery = {}
    for delivery in bundle.deliveries:
        if delivery.table == "offline":
            for row in delivery.records:
                intake_of_delivery[row["id"]] = delivery.intake

    # (shard, request_key) -> 当前胜出观测；同键晚到者胜。
    winners: dict[tuple[str, str], RequestObservation] = {}
    superseded: dict[str, str] = {}
    rows = sorted(
        bundle.table("requests"),
        key=lambda r: (intake_of_delivery.get(r["delivery"], 0), r["id"]),
    )
    for row in rows:
        delivery_id = row["delivery"]
        observation = RequestObservation(
            record_id=row["id"],
            delivery=delivery_id,
            shard=row["shard"],
            request_key=row["request_key"],
            time=int(row["time"]),
            latency=int(row["latency"]),
            status=delivery_status.get(delivery_id, "sample"),
            intake=intake_of_delivery.get(delivery_id, 0),
        )
        key = (observation.shard, observation.request_key)
        prior = winners.get(key)
        if prior is not None:
            superseded[prior.record_id] = observation.record_id
        winners[key] = observation

    return RequestModel(
        version=bundle.version,
        observations=tuple(winners.values()),
        superseded_records=superseded,
    )
