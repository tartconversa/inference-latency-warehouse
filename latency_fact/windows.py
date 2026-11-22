"""考核窗口事实组装。

对每条 policy（客户 × 服务 × shard × 时段），把当前版本下所有可追查证据
归并为一份窗口事实，给出：

* 权威请求数 ``exact_total``（确知连续代内的差分 + 全量离线包）；
* 总量上界 ``total_upper``（再纳入只具上界语义的残段；存在开放缺口时为
  ``None``，表示上界不可知）；
* 慢请求可证区间 ``[slow_lower, slow_upper]``，承诺线穿过桶时只保留区间，
  不做均匀假设、不平均两种桶的百分位；
* 未归属量与未覆盖缺口；
* 合规判定：确实超标 / 确实符合 / 证据不足 / 确证无服务。

窗口先按所有证据边界切成互不重叠的基础段，每段至多绑定一个权威来源，从
机制上杜绝重传、补报与镜像的重复累加。
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .coverage import CoverageModel
from .histograms import HistogramModel
from .intake import SourceBundle
from .model import (
    EvidenceKind,
    EvidenceLine,
    HistogramInterval,
    Policy,
    RequestObservation,
    UnassignedReading,
    Verdict,
    WindowFact,
    intersect,
    ppm,
    rate_breaches,
)
from .requests import RequestModel


@dataclass(frozen=True)
class _Segment:
    span: tuple[int, int]
    exact_total: int
    slow_lower: int
    slow_upper: int
    source_ids: tuple[str, ...]
    intakes: tuple[int, ...]
    kind: EvidenceKind
    detail: str


def load_policies(bundle: SourceBundle) -> list[Policy]:
    policies = []
    for row in bundle.table("policies"):
        policies.append(
            Policy(
                policy_id=row["id"],
                customer=row["customer"],
                service=row["service"],
                shard=row["shard"],
                span=tuple(row["span"]),
                threshold=int(row["threshold"]),
                max_ppm=int(row["max_ppm"]),
            )
        )
    return sorted(policies, key=lambda p: (p.shard, p.span[0], p.policy_id))


def build_window_facts(
    bundle: SourceBundle,
    coverage: CoverageModel,
    histograms: HistogramModel,
    requests: RequestModel,
) -> list[WindowFact]:
    policies = load_policies(bundle)
    return [
        _build_one(policy, coverage, histograms, requests, bundle.version)
        for policy in policies
    ]


def _build_one(
    policy: Policy,
    coverage: CoverageModel,
    histograms: HistogramModel,
    requests: RequestModel,
    version: int,
) -> WindowFact:
    threshold = policy.threshold
    evidence: list[EvidenceLine] = []
    segments = _segments_for(policy, coverage, histograms, requests, threshold, evidence)

    exact_total = sum(seg.exact_total for seg in segments)
    slow_lower = sum(seg.slow_lower for seg in segments)
    # 上界只在同一段内闭合：权威慢上界 + 上界段的整段计数。
    slow_upper = sum(seg.slow_upper for seg in segments)

    # 与窗口相交的未归属读数：单列，不进权威总量。若其候选区间后来被全量
    # 权威覆盖（如离线补报解开了复位之谜），则标记为已化解，不再计未归属量。
    unassigned_total = 0
    for reading in histograms.unassigned_of(policy.shard):
        hit = intersect(reading.candidate_span, policy.span)
        if hit is None:
            continue
        resolved_span = coverage.authority_at(policy.shard, hit)
        resolved = (
            resolved_span is not None and resolved_span.grade == "offline_complete"
        )
        if not resolved:
            unassigned_total += _unassigned_share(reading, policy.span)
        lo_s, up_s = _slow_range_reading(reading, threshold, policy.span)
        evidence.append(
            EvidenceLine(
                kind=EvidenceKind.UNASSIGNED,
                source_id=reading.reading_id,
                span=hit,
                exact_total=None,
                total_upper=reading.total,
                slow_lower=0 if resolved else lo_s,
                slow_upper=up_s,
                detail=(
                    f"{reading.reason}（{reading.stream}/{reading.generation}）"
                    + ("；已被全量离线证据化解，仅留审计痕迹" if resolved else "")
                ),
                intake=reading.intake,
            )
        )

    gaps = tuple(coverage.gaps(policy.shard, policy.span))
    total_upper: int | None
    if gaps:
        total_upper = None  # 开放缺口：任何总量上界都不可知
    else:
        total_upper = slow_upper_cap(segments, exact_total)

    verdict, rate_lo, rate_hi = _verdict(
        policy, exact_total, total_upper, slow_lower, slow_upper, gaps, segments
    )

    credential_id = _credential_id(version, policy, verdict, exact_total, slow_lower)
    return WindowFact(
        version=version,
        policy_id=policy.policy_id,
        customer=policy.customer,
        service=policy.service,
        shard=policy.shard,
        span=policy.span,
        threshold=threshold,
        max_ppm=policy.max_ppm,
        exact_total=exact_total,
        total_upper=total_upper,
        slow_lower=slow_lower,
        slow_upper=slow_upper,
        unassigned_total=unassigned_total,
        uncovered=gaps,
        verdict=verdict,
        rate_lower_ppm=rate_lo,
        rate_upper_ppm=rate_hi,
        evidence=tuple(evidence),
        credential_id=credential_id,
    )


def slow_upper_cap(segments: list[_Segment], exact_total: int) -> int:
    """闭合覆盖下的总量上界：权威量加上界段整段计数。"""
    extra = sum(
        seg.slow_upper - seg.exact_total
        for seg in segments
        if seg.kind == EvidenceKind.HISTOGRAM_PARTIAL
    )
    return exact_total + max(extra, 0)


def _segments_for(
    policy: Policy,
    coverage: CoverageModel,
    histograms: HistogramModel,
    requests: RequestModel,
    threshold: int,
    evidence: list[EvidenceLine],
) -> list[_Segment]:
    window = policy.span
    boundaries = {window[0], window[1]}

    interval_hits: list[tuple[HistogramInterval, tuple[int, int]]] = []
    for interval in histograms.intervals_of(policy.shard):
        hit = intersect(interval.span, window)
        if hit is not None:
            interval_hits.append((interval, hit))
            boundaries.update(hit)

    offline_hits: list[tuple] = []
    for fragment in coverage.fragments_of(policy.shard):
        if fragment.grade != "offline_complete":
            continue
        hit = intersect(fragment.span, window)
        if hit is not None:
            offline_hits.append((fragment, hit))
            boundaries.update(hit)

    ordered = sorted(boundaries)
    cell_spans = list(zip(ordered, ordered[1:]))

    # 全量离线请求按时间预索引（已按 request_key 去重）。
    complete_requests = requests.complete_in_span(policy.shard, window)

    segments: list[_Segment] = []
    for cell in cell_spans:
        segment = _resolve_cell(
            cell,
            window,
            interval_hits,
            offline_hits,
            complete_requests,
            threshold,
            evidence,
        )
        if segment is not None:
            segments.append(segment)
    return segments


def _resolve_cell(
    cell: tuple[int, int],
    window_span: tuple[int, int],
    interval_hits: list[tuple[HistogramInterval, tuple[int, int]]],
    offline_hits: list[tuple],
    complete_requests: list[RequestObservation],
    threshold: int,
    evidence: list[EvidenceLine],
) -> _Segment | None:
    offline_match = [
        fragment
        for fragment, hit in offline_hits
        if hit[0] <= cell[0] and cell[1] <= hit[1]
    ]
    interval_match = [
        (interval, hit)
        for interval, hit in interval_hits
        if hit[0] <= cell[0] and cell[1] <= hit[1]
    ]

    if offline_match:
        fragment = offline_match[0]
        in_cell = [r for r in complete_requests if cell[0] <= r.time < cell[1]]
        total = len(in_cell)
        slow = sum(1 for r in in_cell if r.latency >= threshold)
        evidence.append(
            EvidenceLine(
                kind=EvidenceKind.OFFLINE_COMPLETE,
                source_id=fragment.evidence_id,
                span=cell,
                exact_total=total,
                total_upper=total,
                slow_lower=slow,
                slow_upper=slow,
                detail=(
                    f"全量离线包 {fragment.evidence_id}"
                    + (f"，替换 {fragment.supersedes}" if fragment.supersedes else "")
                ),
                intake=fragment.intake,
            )
        )
        return _Segment(
            span=cell,
            exact_total=total,
            slow_lower=slow,
            slow_upper=slow,
            source_ids=(fragment.evidence_id,),
            intakes=(fragment.intake,),
            kind=EvidenceKind.OFFLINE_COMPLETE,
            detail="offline-complete",
        )

    if interval_match:
        interval, _hit = interval_match[0]
        slow_lo, slow_hi = _slow_range_interval(interval, threshold)
        # 权威条件：差分本身精确，且完整落在窗口内（未被窗口边界裁剪）。
        fully_inside = (
            interval.span[0] >= window_span[0]
            and interval.span[1] <= window_span[1]
        )
        is_exact = interval.exact and fully_inside
        kind = (
            EvidenceKind.HISTOGRAM if is_exact else EvidenceKind.HISTOGRAM_PARTIAL
        )
        evidence.append(
            EvidenceLine(
                kind=kind,
                source_id=interval.interval_id,
                span=cell,
                exact_total=interval.total if is_exact else None,
                total_upper=interval.total,
                slow_lower=slow_lo if is_exact else 0,
                slow_upper=slow_hi,
                detail=(
                    f"{interval.stream}/{interval.generation} 差分 "
                    f"{interval.start_record}→{interval.end_record}"
                    + ("" if is_exact else "（跨窗口/替换残段，仅上界）")
                ),
                intake=interval.intake,
            )
        )
        return _Segment(
            span=cell,
            exact_total=interval.total if is_exact else 0,
            slow_lower=slow_lo if is_exact else 0,
            slow_upper=slow_hi,
            source_ids=(interval.interval_id,),
            intakes=(interval.intake,),
            kind=kind,
            detail="histogram",
        )
    return None


def _slow_range_interval(
    interval: HistogramInterval, threshold: int
) -> tuple[int, int]:
    low = high = 0
    for bucket, count in zip(interval.buckets, interval.bucket_counts):
        b_lo, b_hi = bucket.slow_range(threshold, count)
        low += b_lo
        high += b_hi
    return low, high


def _slow_range_reading(
    reading: UnassignedReading, threshold: int, window: tuple[int, int]
) -> tuple[int, int]:
    # 候选区间与窗口只是部分相交时，读数整体仍只能给整段上界。
    low = high = 0
    for bucket, count in zip(reading.buckets, reading.bucket_counts):
        b_lo, b_hi = bucket.slow_range(threshold, count)
        low += b_lo
        high += b_hi
    return low, high


def _unassigned_share(
    reading: UnassignedReading, window: tuple[int, int]
) -> int:
    return reading.total


def _verdict(
    policy: Policy,
    exact_total: int,
    total_upper: int | None,
    slow_lower: int,
    slow_upper: int,
    gaps: tuple[tuple[int, int], ...],
    segments: list[_Segment],
) -> tuple[Verdict, float | None, float | None]:
    threshold = policy.threshold
    max_ppm = policy.max_ppm

    rate_lo = ppm(slow_lower, exact_total) if exact_total else None
    rate_hi = ppm(slow_upper, total_upper) if total_upper else None

    # 确证无服务：窗口闭合覆盖且权威总量为 0。
    if not gaps and exact_total == 0 and not any(
        seg.kind == EvidenceKind.HISTOGRAM_PARTIAL for seg in segments
    ):
        return Verdict.NO_SERVICE, None, None

    # 正式判定要求整个证明范围闭合：存在开放缺口时，无论数学上能否证明
    # 超标，都只能给“证据不足”并触发补取证，不能出具确定性合规结论。
    if gaps:
        return Verdict.INSUFFICIENT, rate_lo, rate_hi

    # 确实超标：即便取最有利的口径（已确证慢请求 / 最大可能总量）仍超线。
    denominator_for_breach = total_upper if total_upper is not None else exact_total
    if denominator_for_breach > 0 and rate_breaches(
        slow_lower, denominator_for_breach, max_ppm
    ):
        return Verdict.BREACH, rate_lo, rate_hi

    # 确实符合：证明范围闭合，且最不利口径（慢上界 / 权威总量）不超线。
    if all(seg.kind != EvidenceKind.HISTOGRAM_PARTIAL for seg in segments):
        if exact_total == 0 or not rate_breaches(slow_upper, exact_total, max_ppm):
            return Verdict.COMPLIANT, rate_lo, rate_hi

    return Verdict.INSUFFICIENT, rate_lo, rate_hi


def _credential_id(
    version: int,
    policy: Policy,
    verdict: Verdict,
    exact_total: int,
    slow_lower: int,
) -> str:
    digest = hashlib.sha256(
        "|".join(
            [
                str(version),
                policy.policy_id,
                policy.customer,
                policy.service,
                policy.shard,
                f"{policy.span[0]},{policy.span[1]}",
                str(policy.threshold),
                str(policy.max_ppm),
                verdict.value,
                str(exact_total),
                str(slow_lower),
            ]
        ).encode("utf-8")
    ).hexdigest()[:12]
    return f"wf-{digest}"
