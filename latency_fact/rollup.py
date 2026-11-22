"""站点与园区汇总。

* 站点（shard）汇总：合并该 shard 上所有窗口的请求量与慢请求界限，区分
  “确证没有服务的时段”（NO_SERVICE，不进费率分母）和“未能核清的时段”
  （存在覆盖缺口或未归属量）。
* 园区汇总：跨 shard 合并。仍可独立确认的客户窗口保持自身判定，不被另一
  站点缺资料牵连；但跨站点的**正式结论**要求所有相关 shard 都已核清，只要
  有一个 shard 未核清，园区级结论只能是 pending，不能只看完备部分下结论。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .model import Verdict, WindowFact


@dataclass
class ShardSummary:
    version: int
    shard: str
    exact_total: int
    total_upper: int | None
    slow_lower: int
    slow_upper: int
    unassigned_total: int
    no_service_windows: tuple[str, ...]
    breached_windows: tuple[str, ...]
    compliant_windows: tuple[str, ...]
    insufficient_windows: tuple[str, ...]
    unreconciled_spans: tuple[tuple[int, int], ...]
    reconciled: bool

    @property
    def has_gap(self) -> bool:
        return bool(self.unreconciled_spans)


@dataclass
class CampusRollup:
    version: int
    shards: tuple[ShardSummary, ...]
    exact_total: int
    total_upper: int | None
    slow_lower: int
    slow_upper: int
    unassigned_total: int
    breached_windows: tuple[str, ...]
    compliant_windows: tuple[str, ...]
    insufficient_windows: tuple[str, ...]
    no_service_windows: tuple[str, ...]
    # 园区正式结论：reconciled 才允许给整体合规判断
    reconciled: bool
    overall_verdict: Verdict | None
    pending_shards: tuple[str, ...] = field(default_factory=tuple)


def summarize_shard(version: int, shard: str, facts: list[WindowFact]) -> ShardSummary:
    mine = [f for f in facts if f.shard == shard]
    exact_total = sum(f.exact_total or 0 for f in mine)
    slow_lower = sum(f.slow_lower for f in mine)
    slow_upper = sum(f.slow_upper for f in mine)
    unassigned = sum(f.unassigned_total for f in mine)

    uppers = [f.total_upper for f in mine if f.total_upper is not None]
    total_upper = sum(uppers) if len(uppers) == len(mine) else None

    unreconciled_spans = sorted({span for f in mine for span in f.uncovered})

    def ids(verdict: Verdict) -> tuple[str, ...]:
        return tuple(f.policy_id for f in mine if f.verdict is verdict)

    return ShardSummary(
        version=version,
        shard=shard,
        exact_total=exact_total,
        total_upper=total_upper,
        slow_lower=slow_lower,
        slow_upper=slow_upper,
        unassigned_total=unassigned,
        no_service_windows=ids(Verdict.NO_SERVICE),
        breached_windows=ids(Verdict.BREACH),
        compliant_windows=ids(Verdict.COMPLIANT),
        insufficient_windows=ids(Verdict.INSUFFICIENT),
        unreconciled_spans=tuple(unreconciled_spans),
        reconciled=not unreconciled_spans and unassigned == 0,
    )


def rollup_campus(version: int, facts: list[WindowFact]) -> CampusRollup:
    shard_names = sorted({f.shard for f in facts})
    summaries = tuple(
        summarize_shard(version, shard, facts) for shard in shard_names
    )

    exact_total = sum(s.exact_total for s in summaries)
    slow_lower = sum(s.slow_lower for s in summaries)
    slow_upper = sum(s.slow_upper for s in summaries)
    unassigned = sum(s.unassigned_total for s in summaries)
    uppers = [s.total_upper for s in summaries if s.total_upper is not None]
    total_upper = sum(uppers) if len(uppers) == len(summaries) else None

    breached = tuple(p for s in summaries for p in s.breached_windows)
    compliant = tuple(p for s in summaries for p in s.compliant_windows)
    insufficient = tuple(p for s in summaries for p in s.insufficient_windows)
    no_service = tuple(p for s in summaries for p in s.no_service_windows)
    pending = tuple(s.shard for s in summaries if not s.reconciled)

    reconciled = not pending
    if reconciled:
        overall = Verdict.BREACH if breached else Verdict.COMPLIANT
    else:
        overall = None  # 缺资料未核清：正式结论保留，不能只看完备部分

    return CampusRollup(
        version=version,
        shards=summaries,
        exact_total=exact_total,
        total_upper=total_upper,
        slow_lower=slow_lower,
        slow_upper=slow_upper,
        unassigned_total=unassigned,
        breached_windows=breached,
        compliant_windows=compliant,
        insufficient_windows=insufficient,
        no_service_windows=no_service,
        reconciled=reconciled,
        overall_verdict=overall,
        pending_shards=pending,
    )
