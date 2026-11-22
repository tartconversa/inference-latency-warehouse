"""覆盖片段建模。

输入 coverage 与 offline 原件，输出每个 shard 在某版本下具备全量声明的
覆盖片段集合，并处理三类关系：

1. **镜像**：``mirror_of`` 指向另一流的覆盖只是同批请求的另一观测点，证实
   包含关系后不产生新的覆盖片段，只用于交叉印证；镜像范围若超出被镜像流，
   超出部分不具全量效力。
2. **全量替换**：``status=complete`` 的离线包按自身 span 建立权威片段，并
   显式撤销 ``supersedes`` 所指覆盖在该区间的权威（旧直方图证据相应失效，
   不得与离线明细重复累加）。
3. **采样补交**：``status=sample`` 不建立覆盖片段，不能填补全量缺口。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .model import CoverageFragment, intersect, merge_spans, uncovered_within
from .intake import SourceBundle


@dataclass
class MirrorClaim:
    coverage_id: str
    stream: str
    shard: str
    span: tuple[int, int]
    mirror_of: str
    accepted_span: tuple[int, int] | None   # 被被镜像流包含而采信的部分
    rejected_span: tuple[int, int] | None   # 超出被镜像流、无全量效力的部分


@dataclass
class CoverageModel:
    version: int
    fragments: list[CoverageFragment] = field(default_factory=list)
    mirrors: list[MirrorClaim] = field(default_factory=list)
    # 被撤销权威的覆盖 id -> 撤销区间（离线全量包的 span）
    revoked: dict[str, list[tuple[int, int]]] = field(default_factory=dict)

    def shards(self) -> list[str]:
        return sorted({f.shard for f in self.fragments})

    def fragments_of(self, shard: str) -> list[CoverageFragment]:
        return [f for f in self.fragments if f.shard == shard]

    def authoritative_spans(self, shard: str) -> list[tuple[int, int]]:
        """当前仍具全量权威的覆盖区间（已应用替换，已合并）。"""
        return merge_spans([f.span for f in self.fragments_of(shard)])

    def authority_at(self, shard: str, span: tuple[int, int]) -> CoverageFragment | None:
        """返回完整拥有该区间权威的片段；跨片段或无覆盖返回 None。"""
        for fragment in self.fragments_of(shard):
            if fragment.span[0] <= span[0] and span[1] <= fragment.span[1]:
                return fragment
        return None

    def is_revoked(self, coverage_id: str, span: tuple[int, int]) -> bool:
        """某 coverage 记录在给定区间是否已被离线全量包撤销权威。"""
        for revoked_span in self.revoked.get(coverage_id, []):
            if intersect(revoked_span, span) is not None:
                return True
        return False

    def gaps(self, shard: str, window: tuple[int, int]) -> list[tuple[int, int]]:
        return uncovered_within(self.authoritative_spans(shard), window)


def build_coverage(bundle: SourceBundle) -> CoverageModel:
    model = CoverageModel(version=bundle.version)
    coverage_rows = bundle.table("coverage")

    # 先登记独立流覆盖（mirror_of 为空者）。
    independent: dict[str, CoverageFragment] = {}
    stream_coverage: dict[str, str] = {}          # stream -> coverage id
    spans_by_stream: dict[str, list[tuple[int, int]]] = {}
    for row in coverage_rows:
        if row["mirror_of"] is not None:
            continue
        fragment = CoverageFragment(
            fragment_id=row["id"],
            shard=row["shard"],
            span=tuple(row["span"]),
            source=row["stream"],
            grade=row["grade"],
            evidence_id=row["id"],
            intake=_intake_of(bundle, "coverage", row["id"]),
        )
        model.fragments.append(fragment)
        independent[row["id"]] = fragment
        stream_coverage[row["stream"]] = row["id"]
        spans_by_stream.setdefault(row["stream"], []).append(fragment.span)

    # 镜像只做印证：采信被镜像流已覆盖的部分，超出部分标记为无全量效力。
    for row in coverage_rows:
        if row["mirror_of"] is None:
            continue
        target_spans = merge_spans(spans_by_stream.get(row["mirror_of"], []))
        span = tuple(row["span"])
        accepted = merge_spans(
            hit for target in target_spans if (hit := intersect(target, span))
        )
        rejected = uncovered_within(target_spans, span)
        model.mirrors.append(
            MirrorClaim(
                coverage_id=row["id"],
                stream=row["stream"],
                shard=row["shard"],
                span=span,
                mirror_of=row["mirror_of"],
                accepted_span=accepted[0] if accepted else None,
                rejected_span=rejected[0] if rejected else None,
            )
        )

    # 离线交付：complete 建权威片段并撤销旧覆盖；sample 不产生片段。
    for row in bundle.table("offline"):
        if row["status"] != "complete":
            continue
        span = tuple(row["span"])
        fragment = CoverageFragment(
            fragment_id=row["id"],
            shard=row["shard"],
            span=span,
            source=row["id"],
            grade="offline_complete",
            evidence_id=row["id"],
            supersedes=row["supersedes"],
            intake=_intake_of(bundle, "offline", row["id"]),
        )
        model.fragments.append(fragment)
        if row["supersedes"]:
            model.revoked.setdefault(row["supersedes"], []).append(span)

    return model


def _intake_of(bundle: SourceBundle, table: str, record_id: str) -> int:
    for delivery in bundle.deliveries:
        if delivery.table != table:
            continue
        if any(row["id"] == record_id for row in delivery.records):
            return delivery.intake
    return 0
