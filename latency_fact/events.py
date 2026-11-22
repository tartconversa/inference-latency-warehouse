"""服务事件提案、事件台账与回执对账。

事件口径（来源约定）：

* 同一客户、服务（同 shard）下**相接的已确认越界窗口**构成一个连续服务
  问题，全版本内只对应一个事件，不为每张日表各造事件；
* 资料不足只产生补取证请求，不产生事件；
* 新证据使某窗口失去越界依据时，事件按证据影响拆分或撤回，仍成立的部分
  保留，并保留原事件出处（订正链可追查）；
* 已有人工响应记录始终独立保留，不被后续 close/amend 覆盖；
* 回执必须回到某次已发提交的证据号；针对已被取代证据的旧回执，不能关闭
  新发现的缺口，标记为 ``stale``。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .intake import SourceBundle
from .model import Verdict, WindowFact


class ProposalAction(str, Enum):
    OPEN = "open"
    AMEND = "amend"
    WITHDRAW = "withdraw"


class ProposalState(str, Enum):
    ACTIVE = "active"          # 当前版本仍需平台受理的提案
    SUPERSEDED = "superseded"  # 被后续版本的新提案取代
    EVIDENCE_GAP = "evidence_gap"  # 证据不足，仅触发补取证


@dataclass(frozen=True)
class EventGroup:
    """一个连续服务问题（相接的已确认越界窗口）。"""

    group_key: str
    customer: str
    service: str
    shard: str
    policies: tuple[str, ...]
    credential_ids: tuple[str, ...]
    first_version: int
    latest_version: int


@dataclass(frozen=True)
class EventProposal:
    """事实仓向服务响应平台发出的一次证据提案。"""

    proposal_id: str
    version: int
    group_key: str
    customer: str
    service: str
    shard: str
    action: ProposalAction
    policies: tuple[str, ...]
    credential_ids: tuple[str, ...]
    basis: str
    state: ProposalState


@dataclass(frozen=True)
class PlatformSubmission:
    """sent 表中服务台此前实际收到的提交。"""

    submission_id: str
    customer: str
    service: str
    shard: str
    policies: tuple[str, ...]
    intake: int
    action: str


@dataclass(frozen=True)
class PlatformReceipt:
    """receipts 表中的平台回执或人工响应。"""

    receipt_id: str
    submission_id: str
    incident: str
    action: str
    response: str | None
    intake: int
    # 对账结果
    submission_known: bool
    matches_current_evidence: bool
    stale_reason: str | None


@dataclass(frozen=True)
class IncidentLedger:
    """平台事件在事实仓侧的对账视图（人工记录独立保留）。"""

    incident: str
    submissions: tuple[PlatformSubmission, ...]
    receipts: tuple[PlatformReceipt, ...]
    human_responses: tuple[PlatformReceipt, ...]
    current_group: EventGroup | None
    platform_state: str | None          # 平台回执报告的最新动作
    platform_state_stale: bool          # 该状态是否建立在已失效证据上


@dataclass
class EventLedger:
    version: int
    groups: list[EventGroup] = field(default_factory=list)
    proposals: list[EventProposal] = field(default_factory=list)
    submissions: list[PlatformSubmission] = field(default_factory=list)
    receipts: list[PlatformReceipt] = field(default_factory=list)
    incident_ledgers: list[IncidentLedger] = field(default_factory=list)
    evidence_gap_policies: tuple[str, ...] = ()
    # 全历史台账用：每个版本各自的证据不足窗口。
    gap_policies_by_version: dict[int, tuple[str, ...]] = field(default_factory=dict)


def _group_key(customer: str, service: str, shard: str) -> str:
    return f"{customer}|{service}|{shard}"


def breach_groups(facts: list[WindowFact]) -> list[EventGroup]:
    """把当前版本的已确认越界窗口按客户/服务/shard 并组。

    同一主体（客户 × 服务 × shard）下的确认越界窗口同属一个连续服务问题：
    订正使中间窗口暂时失去依据时，两侧仍成立的片段保留在同一事件组内（按
    证据影响拆分记录在提案依据中），不为每张日表各造事件。窗口按时间排序，
    保留相接关系的呈现顺序。
    """
    groups: list[EventGroup] = []
    by_subject: dict[str, list[WindowFact]] = {}
    for fact in facts:
        if fact.verdict is not Verdict.BREACH:
            continue
        by_subject.setdefault(
            _group_key(fact.customer, fact.service, fact.shard), []
        ).append(fact)

    for key, rows in by_subject.items():
        rows.sort(key=lambda f: f.span[0])
        groups.append(_make_group(key, rows))
    return sorted(groups, key=lambda g: (g.shard, g.policies[0]))


def _make_group(key: str, rows: list[WindowFact]) -> EventGroup:
    customer, service, shard = key.split("|")
    return EventGroup(
        group_key=key,
        customer=customer,
        service=service,
        shard=shard,
        policies=tuple(f.policy_id for f in rows),
        credential_ids=tuple(f.credential_id for f in rows),
        first_version=min(f.version for f in rows),
        latest_version=max(f.version for f in rows),
    )


def build_event_ledger(
    bundle: SourceBundle,
    facts_by_version: dict[int, list[WindowFact]],
) -> EventLedger:
    """构建截至 ``bundle.version`` 的事件台账。

    ``facts_by_version`` 应包含从首版到当前版本的全部窗口事实：提案在完整
    版本序列上推演，保证每条提案携带其**真实产生版本**，不会因重建而把历史
    open 误标成新版本首次确认。
    """
    target_version = bundle.version
    all_versions = sorted(facts_by_version)
    ledger = EventLedger(version=target_version)

    # 1) 在完整版本序列上推演连续问题，生成 open/amend/withdraw 提案。
    proposals: list[EventProposal] = []
    previous_groups: dict[str, EventGroup] = {}
    for version in all_versions:
        current = {g.group_key: g for g in breach_groups(facts_by_version[version])}
        for key, group in current.items():
            if key not in previous_groups:
                proposals.append(
                    EventProposal(
                        proposal_id=f"prop-v{version}-{key.replace('|', '_')}-open",
                        version=version,
                        group_key=key,
                        customer=group.customer,
                        service=group.service,
                        shard=group.shard,
                        action=ProposalAction.OPEN,
                        policies=group.policies,
                        credential_ids=group.credential_ids,
                        basis=f"v{version} 首次确认越界窗口 {','.join(group.policies)}",
                        state=ProposalState.ACTIVE,
                    )
                )
            elif previous_groups[key].policies != group.policies:
                proposals.append(
                    EventProposal(
                        proposal_id=f"prop-v{version}-{key.replace('|', '_')}-amend",
                        version=version,
                        group_key=key,
                        customer=group.customer,
                        service=group.service,
                        shard=group.shard,
                        action=ProposalAction.AMEND,
                        policies=group.policies,
                        credential_ids=group.credential_ids,
                        basis=(
                            f"v{version} 证据订正：窗口由 "
                            f"{','.join(previous_groups[key].policies)} 调整为 "
                            f"{','.join(group.policies)}；仍成立部分保留同一事件"
                        ),
                        state=ProposalState.ACTIVE,
                    )
                )
        for key, gone in previous_groups.items():
            if key not in current:
                proposals.append(
                    EventProposal(
                        proposal_id=f"prop-v{version}-{key.replace('|', '_')}-withdraw",
                        version=version,
                        group_key=key,
                        customer=gone.customer,
                        service=gone.service,
                        shard=gone.shard,
                        action=ProposalAction.WITHDRAW,
                        policies=(),
                        credential_ids=(),
                        basis=(
                            f"v{version} 原越界窗口 {','.join(gone.policies)} "
                            "全部失去依据，撤回事件并保留出处"
                        ),
                        state=ProposalState.ACTIVE,
                    )
                )
        previous_groups = current

    # 提案构建时均为 ACTIVE；某版本视角下的 SUPERSEDED 状态由流水线切片决定，
    # 避免对冻结对象原地改写而污染全历史台账。
    ledger.proposals = [p for p in proposals if p.version <= target_version]
    ledger.groups = list(
        {g.group_key: g for g in breach_groups(facts_by_version[target_version])}.values()
    )

    # 2) 当前版本证据不足窗口只触发补取证；同时保留各版本缺口映射。
    ledger.evidence_gap_policies = tuple(
        f.policy_id
        for f in facts_by_version[target_version]
        if f.verdict is Verdict.INSUFFICIENT
    )
    ledger.gap_policies_by_version = {
        version: tuple(
            f.policy_id
            for f in version_facts
            if f.verdict is Verdict.INSUFFICIENT
        )
        for version, version_facts in facts_by_version.items()
    }

    # 3) 平台提交与回执装载、对账（只用截至目标版本已到达的交付）。
    ledger.submissions = _load_submissions(bundle)
    ledger.receipts = _reconcile_receipts(bundle, ledger, facts_by_version)
    ledger.incident_ledgers = _build_incident_ledgers(ledger)
    return ledger


def _load_submissions(bundle: SourceBundle) -> list[PlatformSubmission]:
    submissions = []
    for delivery in bundle.deliveries:
        if delivery.table != "sent":
            continue
        for row in delivery.records:
            submissions.append(
                PlatformSubmission(
                    submission_id=row["id"],
                    customer=row["customer"],
                    service=row["service"],
                    shard=row["shard"],
                    policies=tuple(row["policies"]),
                    intake=delivery.intake,
                    action=row["action"],
                )
            )
    return sorted(submissions, key=lambda s: (s.intake, s.submission_id))


def _reconcile_receipts(
    bundle: SourceBundle,
    ledger: EventLedger,
    facts_by_version: dict[int, list[WindowFact]],
) -> list[PlatformReceipt]:
    by_id = {s.submission_id: s for s in ledger.submissions}
    target_facts = facts_by_version[max(v for v in facts_by_version if v <= ledger.version)]
    current_breach = {
        f.policy_id for f in target_facts if f.verdict is Verdict.BREACH
    }
    receipts: list[PlatformReceipt] = []
    for delivery in bundle.deliveries:
        if delivery.table != "receipts":
            continue
        for row in delivery.records:
            submission = by_id.get(row["submission"])
            known = submission is not None
            stale_reason = None
            matches = False
            if known:
                cited = set(submission.policies)
                still_breaching = cited & current_breach
                if row["action"] == "closed" and not still_breaching:
                    stale_reason = (
                        f"close 所依据的窗口 {sorted(cited)} 已全部订正，"
                        "不能据此关闭当前仍成立的越界"
                    )
                elif row["action"] == "closed" and still_breaching:
                    stale_reason = (
                        f"close 与现行证据冲突：{sorted(still_breaching)} 仍确认越界"
                    )
                else:
                    matches = True
            else:
                stale_reason = f"未知提交号 {row['submission']}，无法回到已发证据"
            receipts.append(
                PlatformReceipt(
                    receipt_id=row["id"],
                    submission_id=row["submission"],
                    incident=row["incident"],
                    action=row["action"],
                    response=row["response"],
                    intake=delivery.intake,
                    submission_known=known,
                    matches_current_evidence=matches,
                    stale_reason=stale_reason,
                )
            )
    return sorted(receipts, key=lambda r: (r.intake, r.receipt_id))


def _build_incident_ledgers(ledger: EventLedger) -> list[IncidentLedger]:
    incidents = sorted({r.incident for r in ledger.receipts})
    out: list[IncidentLedger] = []
    for incident in incidents:
        incident_receipts = [r for r in ledger.receipts if r.incident == incident]
        submission_ids = {r.submission_id for r in incident_receipts}
        submissions = tuple(
            s for s in ledger.submissions if s.submission_id in submission_ids
        )
        human = tuple(r for r in incident_receipts if r.action == "response")
        platform_actions = [
            r for r in incident_receipts if r.action != "response"
        ]
        # 同批交错到达时，只要存在与现行证据冲突的平台动作（典型为针对旧
        # 证据的 close），台账最新状态就必须显式标陈旧，不能被同批 accepted 掩盖。
        stale_actions = [r for r in platform_actions if r.stale_reason]
        if stale_actions:
            latest = sorted(stale_actions, key=lambda r: r.intake)[-1]
        elif platform_actions:
            latest = sorted(platform_actions, key=lambda r: r.intake)[-1]
        else:
            latest = None
        # 当前连续问题：取该事件首个提交涉及的客户/服务/shard。
        current_group = None
        if submissions:
            first = submissions[0]
            key = _group_key(first.customer, first.service, first.shard)
            current_group = next(
                (g for g in ledger.groups if g.group_key == key), None
            )
        stale = bool(latest and latest.stale_reason)
        out.append(
            IncidentLedger(
                incident=incident,
                submissions=submissions,
                receipts=tuple(incident_receipts),
                human_responses=human,
                current_group=current_group,
                platform_state=latest.action if latest else None,
                platform_state_stale=stale,
            )
        )
    return out
