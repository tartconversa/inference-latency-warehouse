"""累计直方图解释。

规则（来自来源约定）：

* 累计端点的计数对应到时刻前已处理的请求，只有**同一 stream、同一
  generation** 且时间有序的端点可以相减；
* 跨采集代（复位、所有者切换）没有可直接相减的连续计数：新代首个端点上
  已经存在的累计量进入 ``UnassignedReading``，候选区间取同流上一代最后
  读数到本端点之间，保留来源但不记入任何窗口的权威总量；
* 差分区间必须完整落在该流拥有的、且未被离线全量包撤销的覆盖内才是权威
  证据；被全量替换覆盖的部分删除（离线明细接管，禁止重复累加），残段只能
  给出上界（``exact=False``）；
* 镜像流（gateway 镜像 edge）与被镜像流看到相同请求，只做计数一致性印证，
  不重复产生证据；
* 每个桶单独保持单调，非单调按来源损坏记录并显式暴露。
"""
from __future__ import annotations

from dataclasses import dataclass

from .coverage import CoverageModel
from .intake import SourceBundle
from .model import (
    Bucket,
    HistogramInterval,
    UnassignedReading,
    intersect,
    parse_bounds,
)


@dataclass
class HistogramModel:
    version: int
    intervals: list[HistogramInterval]
    unassigned: list[UnassignedReading]
    mirror_conflicts: list[str]
    monotonicity_violations: list[str]

    def intervals_of(self, shard: str) -> list[HistogramInterval]:
        return [i for i in self.intervals if i.shard == shard]

    def unassigned_of(self, shard: str) -> list[UnassignedReading]:
        return [u for u in self.unassigned if u.shard == shard]


@dataclass(frozen=True)
class _Endpoint:
    record_id: str
    stream: str
    shard: str
    generation: str
    time: int
    buckets: tuple[Bucket, ...]
    counts: tuple[int, ...]
    intake: int


def _to_endpoints(bundle: SourceBundle) -> list[_Endpoint]:
    endpoints: list[_Endpoint] = []
    for delivery in bundle.deliveries:
        if delivery.table != "histograms":
            continue
        for row in delivery.records:
            bounds = parse_bounds(row["bounds"])
            buckets = tuple(Bucket.from_bounds(i, bounds) for i in range(len(bounds)))
            counts = tuple(int(c) for c in row["counts"])
            endpoints.append(
                _Endpoint(
                    record_id=row["id"],
                    stream=row["stream"],
                    shard=row["shard"],
                    generation=row["generation"],
                    time=int(row["time"]),
                    buckets=buckets,
                    counts=counts,
                    intake=delivery.intake,
                )
            )
    return endpoints


def build_histograms(bundle: SourceBundle, coverage: CoverageModel) -> HistogramModel:
    intervals: list[HistogramInterval] = []
    unassigned: list[UnassignedReading] = []
    conflicts: list[str] = []
    violations: list[str] = []

    endpoints = _to_endpoints(bundle)
    mirror_pairs = _mirror_pairs(bundle)
    mirror_streams = set(mirror_pairs)
    all_times: dict[str, list[int]] = {}
    for endpoint in endpoints:
        all_times.setdefault(endpoint.stream, []).append(endpoint.time)

    grouped: dict[tuple[str, str], list[_Endpoint]] = {}
    for endpoint in endpoints:
        grouped.setdefault((endpoint.stream, endpoint.generation), []).append(endpoint)

    for (stream, generation), chain in grouped.items():
        chain.sort(key=lambda e: e.time)
        _check_monotonicity(chain, violations)
        previous: _Endpoint | None = None
        for endpoint in chain:
            if previous is None:
                if sum(endpoint.counts) > 0:
                    unassigned.append(
                        _baseline_reading(
                            endpoint, stream, generation, all_times.get(stream, [])
                        )
                    )
            elif previous.time >= endpoint.time:
                violations.append(
                    f"{stream}/{generation} 端点时刻无序：{endpoint.record_id}"
                )
            elif endpoint.buckets != previous.buckets:
                unassigned.append(
                    UnassignedReading(
                        reading_id=f"u-{endpoint.record_id}",
                        stream=stream,
                        shard=endpoint.shard,
                        generation=generation,
                        candidate_span=(previous.time, endpoint.time),
                        buckets=endpoint.buckets,
                        bucket_counts=endpoint.counts,
                        reason="相邻端点桶边界变化，无法按桶差分",
                        intake=endpoint.intake,
                    )
                )
            else:
                delta = tuple(
                    endpoint.counts[i] - previous.counts[i]
                    for i in range(len(endpoint.counts))
                )
                if any(d < 0 for d in delta):
                    violations.append(
                        f"{stream}/{generation} {previous.record_id}->"
                        f"{endpoint.record_id} 出现负差分"
                    )
                elif stream not in mirror_streams:
                    _emit_interval(
                        previous, endpoint, delta, coverage, stream, intervals, unassigned
                    )
            previous = endpoint

    _check_mirrors(endpoints, mirror_pairs, conflicts)
    return HistogramModel(
        version=bundle.version,
        intervals=intervals,
        unassigned=unassigned,
        mirror_conflicts=conflicts,
        monotonicity_violations=violations,
    )


def _baseline_reading(
    endpoint: _Endpoint,
    stream: str,
    generation: str,
    stream_times: list[int],
) -> UnassignedReading:
    prior = [t for t in stream_times if t < endpoint.time]
    candidate_lo = max(prior) if prior else endpoint.time
    return UnassignedReading(
        reading_id=f"u-{endpoint.record_id}",
        stream=stream,
        shard=endpoint.shard,
        generation=generation,
        candidate_span=(candidate_lo, endpoint.time),
        buckets=endpoint.buckets,
        bucket_counts=endpoint.counts,
        reason="generation 首个端点携带既有累计，复位/切换前来源不可知",
        intake=endpoint.intake,
    )


def _emit_interval(
    previous: _Endpoint,
    endpoint: _Endpoint,
    delta: tuple[int, ...],
    coverage: CoverageModel,
    stream: str,
    intervals: list[HistogramInterval],
    unassigned: list[UnassignedReading],
) -> None:
    full_span = (previous.time, endpoint.time)
    revoked = _revoked_spans(stream, endpoint.shard, coverage)
    owner = coverage.authority_at(endpoint.shard, full_span)
    owned = owner is not None and owner.source == stream

    hits = [hit for span in revoked if (hit := intersect(span, full_span))]
    if not hits:
        if owned:
            intervals.append(
                HistogramInterval(
                    interval_id=f"h-{previous.record_id}-{endpoint.record_id}",
                    stream=stream,
                    shard=endpoint.shard,
                    generation=endpoint.generation,
                    span=full_span,
                    buckets=endpoint.buckets,
                    bucket_counts=delta,
                    start_record=previous.record_id,
                    end_record=endpoint.record_id,
                    intake=endpoint.intake,
                )
            )
        else:
            unassigned.append(
                UnassignedReading(
                    reading_id=f"u-{endpoint.record_id}",
                    stream=stream,
                    shard=endpoint.shard,
                    generation=endpoint.generation,
                    candidate_span=full_span,
                    buckets=endpoint.buckets,
                    bucket_counts=delta,
                    reason="差分区间未完整落在本流权威覆盖内",
                    intake=endpoint.intake,
                )
            )
        return

    # 与离线全量替换相交：整段被替换则删除，由离线证据接管。
    hits_merged = sorted(hits)
    if len(hits_merged) == 1 and hits_merged[0] == full_span:
        return
    surviving = _subtract(full_span, hits_merged)
    if not surviving:
        return
    if not all(
        (f := coverage.authority_at(endpoint.shard, part)) is not None
        and f.source == stream
        for part in surviving
    ):
        unassigned.append(
            UnassignedReading(
                reading_id=f"u-{endpoint.record_id}",
                stream=stream,
                shard=endpoint.shard,
                generation=endpoint.generation,
                candidate_span=full_span,
                buckets=endpoint.buckets,
                bucket_counts=delta,
                reason="替换残段不再由本流权威拥有",
                intake=endpoint.intake,
            )
        )
        return
    # 残段只保留整段计数作为上界（桶计数仍对应原整段，禁止拆分猜测）。
    intervals.append(
        HistogramInterval(
            interval_id=f"h-{previous.record_id}-{endpoint.record_id}",
            stream=stream,
            shard=endpoint.shard,
            generation=endpoint.generation,
            span=surviving[0],
            buckets=endpoint.buckets,
            bucket_counts=delta,
            start_record=previous.record_id,
            end_record=endpoint.record_id,
            intake=endpoint.intake,
            exact=False,
        )
    )


def _subtract(
    span: tuple[int, int], cuts: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    pieces = [span]
    for c_lo, c_hi in cuts:
        next_pieces: list[tuple[int, int]] = []
        for lo, hi in pieces:
            if c_hi <= lo or c_lo >= hi:
                next_pieces.append((lo, hi))
            else:
                if lo < c_lo:
                    next_pieces.append((lo, c_lo))
                if c_hi < hi:
                    next_pieces.append((c_hi, hi))
        pieces = next_pieces
    return pieces


def _revoked_spans(
    stream: str, shard: str, coverage: CoverageModel
) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    for fragment in coverage.fragments_of(shard):
        if fragment.source == stream and fragment.grade == "complete":
            spans.extend(coverage.revoked.get(fragment.evidence_id, []))
    return spans


def _check_monotonicity(chain: list[_Endpoint], violations: list[str]) -> None:
    for earlier, later in zip(chain, chain[1:]):
        if earlier.buckets != later.buckets:
            continue
        for i in range(len(later.counts)):
            if later.counts[i] < earlier.counts[i]:
                violations.append(
                    f"{later.stream}/{later.generation} 桶 {i} 在 "
                    f"{earlier.record_id}->{later.record_id} 间累计倒退"
                )


def _mirror_pairs(bundle: SourceBundle) -> dict[str, str]:
    """镜像 stream -> 被镜像 stream。"""
    return {
        row["stream"]: row["mirror_of"]
        for row in bundle.table("coverage")
        if row["mirror_of"]
    }


def _check_mirrors(
    endpoints: list[_Endpoint],
    mirror_of: dict[str, str],
    conflicts: list[str],
) -> None:
    indexed: dict[tuple[str, int], _Endpoint] = {
        (e.stream, e.time): e for e in endpoints
    }
    for endpoint in endpoints:
        target_stream = mirror_of.get(endpoint.stream)
        if target_stream is None:
            continue
        twin = indexed.get((target_stream, endpoint.time))
        if twin is None or twin.buckets != endpoint.buckets:
            continue
        if twin.counts != endpoint.counts:
            conflicts.append(
                f"镜像 {endpoint.stream} 与 {target_stream} 在 t={endpoint.time} 计数不一致"
            )
