"""版本构建流水线。

一次 ``build_version_artifacts`` 跑完单个资料版本的全部阶段：
覆盖 → 直方图 → 离线请求 → 窗口事实 → 园区汇总 → 事件台账。
事件台账需要全部历史版本的窗口事实，因此另提供 ``build_all_versions``
统一产出，保证同一批重放下各版本口径一致。
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from pathlib import Path

from .coverage import build_coverage
from .events import EventLedger, ProposalState, build_event_ledger
from .histograms import build_histograms
from .intake import SourceBundle, iter_versions
from .model import WindowFact
from .requests import build_requests
from .rollup import CampusRollup, rollup_campus
from .windows import build_window_facts


@dataclass
class VersionArtifacts:
    bundle: SourceBundle
    facts: list[WindowFact]
    rollup: CampusRollup
    event_ledger: EventLedger
    coverage: object
    histograms: object
    requests: object


def build_version_artifacts(bundle: SourceBundle) -> VersionArtifacts:
    coverage = build_coverage(bundle)
    histograms = build_histograms(bundle, coverage)
    requests = build_requests(bundle)
    facts = build_window_facts(bundle, coverage, histograms, requests)
    rollup = rollup_campus(bundle.version, facts)

    # 单版本构建时，事件台账仅基于本版本（主要用于临时探查）。
    ledger = build_event_ledger(bundle, {bundle.version: facts})
    return VersionArtifacts(
        bundle=bundle,
        facts=facts,
        rollup=rollup,
        event_ledger=ledger,
        coverage=coverage,
        histograms=histograms,
        requests=requests,
    )


def build_all_versions(source_dir: Path) -> list[VersionArtifacts]:
    """重放全部到达批次。

    窗口事实按版本独立推导并全部保留；事件台账在**完整版本序列上只推演一次**
    （提案因此携带真实产生版本、open/amend/withdraw 完整），再按各版本切片
    挂回对应产物，保证回查任一版本看到的都是同一条订正链。
    """
    bundles = list(iter_versions(Path(source_dir)))
    facts_by_version: dict[int, list[WindowFact]] = {}
    partial: list[VersionArtifacts] = []

    for bundle in bundles:
        coverage = build_coverage(bundle)
        histograms = build_histograms(bundle, coverage)
        request_model = build_requests(bundle)
        facts = build_window_facts(bundle, coverage, histograms, request_model)
        rollup = rollup_campus(bundle.version, facts)
        facts_by_version[bundle.version] = facts
        partial.append(
            VersionArtifacts(
                bundle=bundle,
                facts=facts,
                rollup=rollup,
                event_ledger=EventLedger(version=bundle.version),
                coverage=coverage,
                histograms=histograms,
                requests=request_model,
            )
        )

    # 全历史台账：一次性得到全部提案与回执对账。
    full_bundle = bundles[-1]
    full_ledger = build_event_ledger(full_bundle, dict(facts_by_version))

    for index, bundle in enumerate(bundles):
        partial[index].event_ledger = _slice_ledger(full_ledger, bundle.version)
    return partial


def _slice_ledger(full_ledger, version: int) -> EventLedger:
    """把全历史事件台账切片到指定版本：保留截至该版本的提案与当版状态。"""
    from .model import WindowFact  # noqa: F401  (类型可读性)

    proposals = []
    for proposal in full_ledger.proposals:
        if proposal.version > version:
            continue
        # 在切片时点，同组若已有更新提案，则本提案在该版本已被取代。
        superseded_by_later = any(
            other.group_key == proposal.group_key
            and other.version > proposal.version
            and other.version <= version
            for other in full_ledger.proposals
        )
        proposals.append(
            dataclasses.replace(
                proposal,
                state=(
                    ProposalState.SUPERSEDED
                    if superseded_by_later
                    else ProposalState.ACTIVE
                ),
            )
        )

    return EventLedger(
        version=version,
        groups=[g for g in full_ledger.groups if g.first_version <= version],
        proposals=proposals,
        submissions=[s for s in full_ledger.submissions if s.intake <= version],
        receipts=[r for r in full_ledger.receipts if r.intake <= version],
        incident_ledgers=[
            ledger
            for ledger in full_ledger.incident_ledgers
            if any(r.intake <= version for r in ledger.receipts)
        ],
        evidence_gap_policies=full_ledger.gap_policies_by_version.get(version, ()),
        gap_policies_by_version={
            v: gaps
            for v, gaps in full_ledger.gap_policies_by_version.items()
            if v <= version
        },
    )
