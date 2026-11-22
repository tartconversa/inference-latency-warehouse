"""领域数据结构与纯函数运算。

时间约定
========
所有业务时刻是整数刻度（毫秒轴之外的请求处理时刻），区间一律半开
``[start, end)``：

* 覆盖 ``span`` 表示流在该区间内对 shard 的观测是完整的；
* 直方图端点 ``time=t`` 的计数，对应该端点之前（请求时刻 ``< t``）已处理
  的请求，端点本身是零读数的锚点，不承载任何请求；
* 同一采集代内两个端点相减，得到区间 ``[t0, t1)`` 内新增请求；
* 考核窗口、离线补交范围同为半开区间，可直接相交、裁剪。

桶约定
======
桶边界为整数毫秒，末桶为 ``inf``。相邻边界 ``(lo, hi]`` 表示延时大于
``lo`` 且小于等于 ``hi`` 的请求，首桶下界为 0。承诺线（threshold）落在
桶内时，该桶只能贡献可证的慢请求上下界，禁止均匀假设。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence


# --------------------------------------------------------------------------- 区间
def intersect(a: tuple[int, int], b: tuple[int, int]) -> Optional[tuple[int, int]]:
    """两个半开区间的交集；不相交或相接（零宽）返回 None。"""
    lo = max(a[0], b[0])
    hi = min(a[1], b[1])
    if lo < hi:
        return lo, hi
    return None


def contains(outer: tuple[int, int], inner: tuple[int, int]) -> bool:
    """outer 是否完整包含 inner（零宽区间视为被包含）。"""
    return outer[0] <= inner[0] and inner[1] <= outer[1]


def merge_spans(spans: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    """把可能相接或重叠的区间合并为连续区间列表。"""
    ordered = sorted(spans)
    merged: list[tuple[int, int]] = []
    for span in ordered:
        if not merged or span[0] > merged[-1][1]:
            merged.append((span[0], span[1]))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], span[1]))
    return merged


def covered_within(
    cover: Sequence[tuple[int, int]], window: tuple[int, int]
) -> list[tuple[int, int]]:
    """窗口内被覆盖的部分。"""
    pieces = []
    for span in cover:
        hit = intersect(span, window)
        if hit is not None:
            pieces.append(hit)
    return merge_spans(pieces)


def uncovered_within(
    cover: Sequence[tuple[int, int]], window: tuple[int, int]
) -> list[tuple[int, int]]:
    """窗口内未被任何覆盖声明保护的缺口（含仅有采样、无全量声明的部分）。"""
    covered = covered_within(cover, window)
    gaps: list[tuple[int, int]] = []
    cursor = window[0]
    for lo, hi in covered:
        if lo > cursor:
            gaps.append((cursor, lo))
        cursor = max(cursor, hi)
    if cursor < window[1]:
        gaps.append((cursor, window[1]))
    return gaps


# --------------------------------------------------------------------------- 枚举
class Verdict(str, Enum):
    """窗口合规判定。"""

    BREACH = "breach"          # 确实超标：可证慢请求下界已使费率超过承诺
    COMPLIANT = "compliant"    # 确实符合：全证明范围成立且费率上界不超承诺
    INSUFFICIENT = "insufficient"  # 证据不足：两侧都不能证明
    NO_SERVICE = "no_service"  # 确证无服务的空窗口，不进入费率分母


class EvidenceKind(str, Enum):
    """证据行类型（决定该行如何计入窗口界限）。"""

    HISTOGRAM = "histogram"          # 代内连续差分，整段权威
    HISTOGRAM_PARTIAL = "histogram_partial"  # 被窗口裁剪的部分，只能给上界
    OFFLINE_COMPLETE = "offline_complete"    # 全量离线包，整段权威
    OFFLINE_SAMPLE = "offline_sample"        # 采样明细，只证存在性
    UNASSIGNED = "unassigned"        # 跨代/跨所有者读数，归属未定


# --------------------------------------------------------------------------- 桶
@dataclass(frozen=True)
class Bucket:
    """一个延时桶 ``(lo, hi]``，hi 为 None 表示正无穷。"""

    lo: int
    hi: Optional[int]

    @classmethod
    def from_bounds(cls, index: int, bounds: Sequence[float]) -> "Bucket":
        lo = 0 if index == 0 else int(bounds[index - 1])
        raw_hi = bounds[index]
        hi = None if raw_hi == float("inf") else int(raw_hi)
        return cls(lo, hi)

    def slow_range(self, threshold: int, count: int) -> tuple[int, int]:
        """给定桶内请求数，返回其中慢请求（latency >= threshold）的可证上下界。

        * 整桶都在承诺线之上（lo 已达线）：全部慢；
        * 整桶都在承诺线之下（hi 存在且 hi < threshold）：全部不慢；
        * 承诺线穿过桶：只能给 [0, count]，禁止均匀假设。
        """
        if self.lo >= threshold:
            return count, count
        if self.hi is not None and self.hi < threshold:
            return 0, 0
        return 0, count


def parse_bounds(raw: Sequence[object]) -> tuple[float, ...]:
    """把源里的边界（整数字符串或 ``inf``）解析为数值元组。"""
    parsed = []
    for item in raw:
        if item == "inf" or item == float("inf"):
            parsed.append(float("inf"))
        else:
            parsed.append(int(item))
    return tuple(parsed)


# --------------------------------------------------------------------------- 费率
def ppm(numerator: int, denominator: int) -> Optional[float]:
    """慢请求百万分比；分母为 0 时无定义。"""
    if denominator == 0:
        return None
    return numerator * 1_000_000 / denominator


def rate_breaches(slow: int, total: int, max_ppm: int) -> bool:
    """精确整数比较：slow/total 是否严格超过 max_ppm（ppm 口径）。"""
    if total == 0:
        return False
    return slow * 1_000_000 > max_ppm * total


# --------------------------------------------------------------------------- 事实载体
@dataclass(frozen=True)
class CoverageFragment:
    """一个对某 shard 具备全量声明的覆盖片段。"""

    fragment_id: str
    shard: str
    span: tuple[int, int]
    source: str                 # stream 名或离线交付 id
    grade: str                  # complete / offline_complete
    evidence_id: str            # 源记录 id，用于追查
    supersedes: Optional[str] = None  # 被明确替换的覆盖 id
    intake: int = 0


@dataclass(frozen=True)
class HistogramInterval:
    """同一采集代内两个端点相减得到的连续区间证据。"""

    interval_id: str
    stream: str
    shard: str
    generation: str
    span: tuple[int, int]
    buckets: tuple[Bucket, ...]
    bucket_counts: tuple[int, ...]
    start_record: str
    end_record: str
    intake: int
    exact: bool = True          # False 表示端点未对齐覆盖，只能作上界使用

    @property
    def total(self) -> int:
        return sum(self.bucket_counts)


@dataclass(frozen=True)
class UnassignedReading:
    """无法在确知连续的运行代/所有者内解释的计数。

    典型来源：新代首个端点的既有累计、跨复位的读差、覆盖窗口落在读数之间。
    这些量保留可追查的来源与候选区间，但不进任何窗口的权威总量。
    """

    reading_id: str
    stream: str
    shard: str
    generation: str
    candidate_span: tuple[int, int]
    buckets: tuple[Bucket, ...]
    bucket_counts: tuple[int, ...]
    reason: str
    intake: int

    @property
    def total(self) -> int:
        return sum(self.bucket_counts)


@dataclass(frozen=True)
class RequestObservation:
    """离线包里的单请求观测。"""

    record_id: str
    delivery: str
    shard: str
    request_key: str
    time: int
    latency: int
    status: str                 # complete / sample
    intake: int


@dataclass(frozen=True)
class Policy:
    """合同考核窗口。"""

    policy_id: str
    customer: str
    service: str
    shard: str
    span: tuple[int, int]
    threshold: int
    max_ppm: int


@dataclass(frozen=True)
class EvidenceLine:
    """窗口事实表里的一行证据：使用者可逐行追查结论来源。"""

    kind: EvidenceKind
    source_id: str
    span: tuple[int, int]
    exact_total: Optional[int]       # 整段权威时的请求数；否则 None
    total_upper: Optional[int]       # 该行能贡献的总量上界
    slow_lower: int                  # 该行可证的慢请求下界
    slow_upper: int                  # 该行慢请求上界
    detail: str                      # 人类可读的追查说明
    intake: int


@dataclass(frozen=True)
class WindowFact:
    """单个考核窗口在某个资料版本下的事实结论。"""

    version: int
    policy_id: str
    customer: str
    service: str
    shard: str
    span: tuple[int, int]
    threshold: int
    max_ppm: int
    exact_total: Optional[int]       # 权威证据给出的确知请求数
    total_upper: Optional[int]       # 含部分证据/未归属候选后的总量上界（缺口开放时 None）
    slow_lower: int
    slow_upper: int
    unassigned_total: int            # 与窗口相交但归属未定的量
    uncovered: tuple[tuple[int, int], ...]
    verdict: Verdict
    rate_lower_ppm: Optional[float]  # 可证费率区间（下界/上界）
    rate_upper_ppm: Optional[float]
    evidence: tuple[EvidenceLine, ...] = field(default_factory=tuple)
    credential_id: str = ""

    @property
    def fully_covered(self) -> bool:
        return not self.uncovered
