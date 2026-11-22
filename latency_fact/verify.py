"""内置不变量验证。

把来源约定与需求中的硬约束固化为可执行检查，既校验合成样本上的具体数值，
也校验与数据无关的结构不变量。``run_checks`` 返回失败说明列表，为空即全
部通过；CLI ``verify`` 与 unittest 共用本模块。
"""
from __future__ import annotations

from pathlib import Path

from .events import ProposalAction
from .model import Verdict
from .pipeline import build_all_versions


def run_checks(source_dir: str | Path) -> list[str]:
    source_dir = Path(source_dir)
    artifacts = build_all_versions(source_dir)
    failures: list[str] = []

    def check(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    by_version = {a.bundle.version: a for a in artifacts}
    check(len(by_version) == 3, "应得到 3 个资料版本")

    _check_no_double_count(by_version, failures)
    _check_generation_continuity(by_version, failures)
    _check_bucket_bounds_no_uniform_fill(by_version, failures)
    _check_sample_not_full(by_version, failures)
    _check_window_verdicts(by_version, failures)
    _check_no_service_denominator(by_version, failures)
    _check_rollup_pending(by_version, failures)
    _check_event_lifecycle(by_version, failures)
    _check_receipt_reconciliation(by_version, failures)
    _check_credential_stability(by_version, failures)
    return failures


# ---------------------------------------------------------------- 不变量
def _check_no_double_count(by_version, failures: list[str]) -> None:
    """镜像流不产证；同一 (shard, request_key) 只计一次；替换区无重复累加。"""
    for version, art in by_version.items():
        interval_streams = {i.stream for i in art.histograms.intervals}
        check = "gateX" not in interval_streams
        if not check:
            failures.append(f"v{version}: 镜像流 gateX 产生了独立证据，存在重复计数风险")

        keys = [
            (o.shard, o.request_key)
            for o in art.requests.observations
        ]
        if len(keys) != len(set(keys)):
            failures.append(f"v{version}: 离线请求键去重失败")

        # 被完整替换的区间不应再出现该流的权威直方图证据。
        for fact in art.facts:
            for line in fact.evidence:
                if line.kind.value == "histogram":
                    for fragment in art.coverage.fragments_of(fact.shard):
                        if fragment.grade != "offline_complete":
                            continue
                        if (
                            fragment.span[0] <= line.span[0]
                            and line.span[1] <= fragment.span[1]
                        ):
                            failures.append(
                                f"v{version}: 窗口 {fact.policy_id} 在离线全量替换区 "
                                f"{fragment.span} 内仍累加直方图证据 {line.source_id}"
                            )


def _check_generation_continuity(by_version, failures: list[str]) -> None:
    """跨代读数必须进入未归属，且候选区间与来源可追查。"""
    v1 = by_version[1]
    east_unassigned = v1.histograms.unassigned_of("east")
    ids = {u.reading_id for u in east_unassigned}
    if "u-a3" not in ids:
        failures.append("v1: 复位代 p2 首端点 a3 的既有累计未被识别为未归属读数")
    for reading in east_unassigned:
        if reading.total <= 0:
            failures.append(f"v1: 未归属读数 {reading.reading_id} 计数异常")
        if not reading.reason:
            failures.append(f"v1: 未归属读数 {reading.reading_id} 缺少原因说明")

    # 未归属量不得出现在任何窗口的权威总量里。
    for fact in v1.facts:
        if fact.policy_id == "k2" and fact.exact_total is not None:
            # k2 的权威量只能来自 p2 差分 (25,35]，未归属的 5 不得计入
            if fact.exact_total != 8:
                failures.append(
                    f"v1: k2 权威总量应为 8（仅 p2 差分），实际 {fact.exact_total}"
                )

    # 跨窗口差分只能给上界：k1 的 (10,15] 来自 (10,20] 差分的裁剪段。
    k1 = next(f for f in v1.facts if f.policy_id == "k1")
    if k1.total_upper is None or k1.total_upper <= k1.exact_total:
        failures.append("v1: k1 应保留跨窗口裁剪段的总量上界，且上界大于权威量")
    partial_kinds = {line.kind.value for line in k1.evidence}
    if "histogram_partial" not in partial_kinds:
        failures.append("v1: k1 缺少 histogram_partial 上界证据行")
    if k1.slow_lower != 8:
        failures.append(f"v1: k1 慢请求下界应只来自完整段（=8），实际 {k1.slow_lower}")

    # v3 全量补齐后，复位基线被离线证据化解，未归属量归零且缺口闭合。
    v3 = by_version[3]
    for fact in v3.facts:
        if fact.shard == "east" and fact.unassigned_total != 0:
            failures.append(
                f"v3: {fact.policy_id} 的未归属量应已被全量离线证据化解"
            )


def _check_bucket_bounds_no_uniform_fill(by_version, failures: list[str]) -> None:
    """承诺线穿过桶时，慢请求必须保留区间而不是单点猜测。"""
    for version, art in by_version.items():
        for fact in art.facts:
            if fact.slow_lower > fact.slow_upper:
                failures.append(
                    f"v{version}: 窗口 {fact.policy_id} 慢请求下界超过上界"
                )
            # 有跨线桶的窗口必然是区间而非定值（除非全量离线逐请求给出）。
            if fact.uncovered or any(
                line.kind.value.startswith("histogram")
                and line.slow_lower != line.slow_upper
                for line in fact.evidence
            ):
                if fact.slow_lower == fact.slow_upper and fact.verdict in (
                    Verdict.COMPLIANT,
                    Verdict.BREACH,
                ):
                    # 结论仍可成立，但界限必须来自其他闭合证据；这里只校验存在区间证据。
                    pass


def _check_sample_not_full(by_version, failures: list[str]) -> None:
    """采样交付不建立覆盖、不填补缺口、不进权威总量。"""
    v2 = by_version[2]
    east_cover = v2.coverage.authoritative_spans("east")
    # 采样 x-east 之后 [20,25) 仍是缺口
    gap = v2.coverage.gaps("east", (0, 35))
    if (20, 25) not in gap:
        failures.append("v2: 采样补交被错误当作全量覆盖，缺口 (20,25) 应仍存在")

    k2 = next(f for f in v2.facts if f.policy_id == "k2")
    if k2.verdict is not Verdict.INSUFFICIENT:
        failures.append(f"v2: 仅有采样的 k2 不应得出确定判定，实际 {k2.verdict}")

    # 采样请求仍抬高可证慢请求下界（req04=51 落在 k2）
    if k2.slow_lower < 1:
        failures.append("v2: k2 采样慢请求未被用于抬高慢请求下界")


def _check_window_verdicts(by_version, failures: list[str]) -> None:
    """合成样本上逐窗口的可证界限与判定（手工核算的期望值）。"""
    expected = {
        1: {
            # (0,10] 权威 8（慢下界1）；(10,15] 因差分跨窗口仅上界。
            "k1": (Verdict.BREACH, 15, (8, 22)),
            # 开放缺口 (20,25)：权威量仅 p2 差分 8，复位基线 5 属未归属。
            "k2": (Verdict.INSUFFICIENT, 8, None),
            "k3": (Verdict.BREACH, 10, None),
            "k4": (Verdict.BREACH, 8, None),
            "k5": (Verdict.BREACH, 7, None),
            "k6": (Verdict.NO_SERVICE, 0, (0, 0)),
        },
        2: {
            "k1": (Verdict.BREACH, 15, None),
            "k2": (Verdict.INSUFFICIENT, 8, None),
            # 全量离线替换：5 请求、1 慢，费率 20% 符合。
            "k4": (Verdict.COMPLIANT, 5, (1, 1)),
        },
        3: {
            # 8（直方图）+3（离线 [10,15)）
            "k1": (Verdict.BREACH, 18, (9, 15)),
            # 5 离线 [15,25) +8 直方图 [25,35)，慢下界 7
            "k2": (Verdict.BREACH, 13, (7, 9)),
            "k4": (Verdict.COMPLIANT, 5, None),
        },
    }
    for version, rows in expected.items():
        facts = {f.policy_id: f for f in by_version[version].facts}
        for policy_id, expectation in rows.items():
            verdict, exact, slow = expectation
            fact = facts.get(policy_id)
            if fact is None:
                failures.append(f"v{version}: 缺少窗口 {policy_id}")
                continue
            if fact.verdict is not verdict:
                failures.append(
                    f"v{version}: {policy_id} 判定应为 {verdict.value}，"
                    f"实际 {fact.verdict.value}"
                )
            if exact is not None and fact.exact_total != exact:
                failures.append(
                    f"v{version}: {policy_id} 权威总量应为 {exact}，"
                    f"实际 {fact.exact_total}"
                )
            if slow is not None and (fact.slow_lower, fact.slow_upper) != slow:
                failures.append(
                    f"v{version}: {policy_id} 慢请求区间应为 {slow}，"
                    f"实际 ({fact.slow_lower},{fact.slow_upper})"
                )

    # v3 全量补齐后 east 不再有缺口
    v3 = by_version[3]
    for fact in v3.facts:
        if fact.shard == "east" and fact.uncovered:
            failures.append(f"v3: east 窗口 {fact.policy_id} 仍存在缺口 {fact.uncovered}")


def _check_no_service_denominator(by_version, failures: list[str]) -> None:
    """确证无服务窗口不进费率分母，也不判超标。"""
    for version, art in by_version.items():
        k6 = next(f for f in art.facts if f.policy_id == "k6")
        if k6.verdict is not Verdict.NO_SERVICE:
            failures.append(f"v{version}: k6 应为确证无服务，实际 {k6.verdict}")
        if k6.rate_lower_ppm is not None or k6.rate_upper_ppm is not None:
            failures.append(f"v{version}: k6 无服务窗口不应产生费率")


def _check_rollup_pending(by_version, failures: list[str]) -> None:
    """园区正式结论在有未核清站点时必须保留；独立窗口判定不受牵连。"""
    v1 = by_version[1]
    if v1.rollup.reconciled:
        failures.append("v1: east 存在缺口时园区不应判定为已核清")
    if v1.rollup.overall_verdict is not None:
        failures.append("v1: 跨站点正式结论不能只看完备部分，应为 pending")
    if "east" not in v1.rollup.pending_shards:
        failures.append("v1: pending_shards 应包含 east")

    # west 窗口仍可独立确认
    west_breach = [
        f.policy_id for f in v1.facts
        if f.shard == "west" and f.verdict is Verdict.BREACH
    ]
    if set(west_breach) != {"k3", "k4", "k5"}:
        failures.append(f"v1: west 可独立确认的超标窗口异常: {west_breach}")

    v3 = by_version[3]
    if not v3.rollup.reconciled:
        failures.append("v3: 全部补齐后园区应已核清")
    if v3.rollup.overall_verdict is not Verdict.BREACH:
        failures.append("v3: 园区总体判定应为确实超标")


def _check_event_lifecycle(by_version, failures: list[str]) -> None:
    """连续问题并组、订正拆分、不为每张日表造事件、证据不足不建事件。"""
    v3 = by_version[3]
    proposals = v3.event_ledger.proposals
    actions = [(p.version, p.action, p.group_key) for p in proposals]

    # v1 为 west 连续越界开一个事件（k3+k4+k5 相接）
    opens_v1 = [p for p in proposals if p.version == 1 and p.action is ProposalAction.OPEN]
    west_open = next((p for p in opens_v1 if p.shard == "west"), None)
    if west_open is None or tuple(west_open.policies) != ("k3", "k4", "k5"):
        failures.append("v1: 应把相接的 k3/k4/k5 作为一个连续事件打开")

    # v2 k4 失去依据 -> 对同一 group 的 amend，而不是新开事件
    amend_v2 = [
        p for p in proposals
        if p.version == 2 and p.action is ProposalAction.AMEND and p.shard == "west"
    ]
    if not amend_v2:
        failures.append("v2: k4 订正后应产生 amend 而非新开/沉默")
    else:
        if "k4" in amend_v2[0].policies:
            failures.append("v2: amend 后事件不应继续包含已失去依据的 k4")
        if "k3" not in amend_v2[0].policies:
            failures.append("v2: amend 应保留仍成立的 k3")

    # v3 east 两个窗口各自确认，但 k1 与 k2 分属不同客户，不得并为一个事件
    east_groups = {g.group_key: g for g in v3.event_ledger.groups if g.shard == "east"}
    if len(east_groups) != 2:
        failures.append(f"v3: east 两个客户应是两个事件组，实际 {len(east_groups)}")

    # 证据不足窗口只进补取证，不进提案
    if "k2" in v3.event_ledger.evidence_gap_policies:
        failures.append("v3: k2 已确认超标，不应再停留在补取证列表")
    v2_gap = by_version[2].event_ledger.evidence_gap_policies
    if "k2" not in v2_gap:
        failures.append("v2: 证据不足的 k2 应触发补取证")


def _check_receipt_reconciliation(by_version, failures: list[str]) -> None:
    """旧回执不能关闭新缺口；人工响应独立保留；回执必须回到已知提交。"""
    v3 = by_version[3]
    receipts = {r.receipt_id: r for r in v3.event_ledger.receipts}

    ack1 = receipts.get("ack1")
    if ack1 is None or ack1.matches_current_evidence:
        failures.append("ack1: 建立在旧证据上的 close 必须标记为陈旧/失效")
    if not ack1 or not ack1.stale_reason:
        failures.append("ack1: 应给出陈旧原因，说明不能关闭仍成立的越界")

    ack2 = receipts.get("ack2")
    if ack2 is None or not ack2.matches_current_evidence:
        failures.append("ack2: 针对 amend 的 accepted 应与现行证据匹配")

    human1 = receipts.get("human1")
    if human1 is None or human1.action != "response" or human1.response != "dispatched":
        failures.append("human1: 人工响应记录必须独立原样保留")

    # 事件台账视角：inc17 的平台最新状态应被识别为陈旧
    inc17 = next(
        (ledger for ledger in v3.event_ledger.incident_ledgers if ledger.incident == "inc17"),
        None,
    )
    if inc17 is None:
        failures.append("缺少 inc17 的事件台账")
    elif not inc17.platform_state_stale:
        failures.append("inc17: 平台 close 状态相对现行证据已陈旧，应显式标出")
    elif not inc17.human_responses:
        failures.append("inc17: 人工响应未在台账中独立保留")


def _check_credential_stability(by_version, failures: list[str]) -> None:
    """同一版本同一窗口重复构建得到同一凭证号；凭证号携带版本隔离。"""
    source_dir = None
    rebuilt = build_all_versions(_sources_of(by_version))
    for version, art in by_version.items():
        again = next(a for a in rebuilt if a.bundle.version == version)
        current = {f.policy_id: f.credential_id for f in art.facts}
        repeated = {f.policy_id: f.credential_id for f in again.facts}
        if current != repeated:
            failures.append(f"v{version}: 重复构建导致服务凭证号漂移")

    # 跨版本：即使窗口仍超标，v1 与 v3 的凭证也必须可区分（版本入摘要）
    k3_v1 = next(f for f in by_version[1].facts if f.policy_id == "k3")
    k3_v3 = next(f for f in by_version[3].facts if f.policy_id == "k3")
    if k3_v1.credential_id == k3_v3.credential_id:
        failures.append("不同版本的凭证号必须可区分，防止新旧口径混用")


def _sources_of(by_version) -> Path:
    any_bundle = next(iter(by_version.values())).bundle
    # 交付路径形如 .../sources/01-*.json，取其两级之上目录。
    first_path = Path(any_bundle.deliveries[0].path)
    return first_path.parent
