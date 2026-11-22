"""基于合成源资料的端到端流水线测试。

覆盖需求硬约束：镜像去重、代内连续性与未归属、采样不补全量、离线替换、
窗口可证界限、合规判定、园区 pending、事件生命周期与回执对账。
"""
import unittest
from pathlib import Path

from latency_fact.events import ProposalAction, ProposalState
from latency_fact.model import EvidenceKind, Verdict
from latency_fact.pipeline import build_all_versions, build_version_artifacts
from latency_fact.intake import load_version, iter_versions

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ROOT / "sources"


class PipelineTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = {a.bundle.version: a for a in build_all_versions(SOURCES)}

    def fact(self, version, policy_id):
        return next(f for f in self.artifacts[version].facts if f.policy_id == policy_id)


class CoverageAndMirrorTests(PipelineTestBase):
    def test_mirror_stream_produces_no_independent_evidence(self):
        for art in self.artifacts.values():
            self.assertNotIn("gateX", {i.stream for i in art.histograms.intervals})
            self.assertEqual(art.histograms.mirror_conflicts, [])

    def test_mirror_claim_is_recorded_but_not_new_coverage(self):
        coverage = self.artifacts[1].coverage
        mirror = next(m for m in coverage.mirrors if m.coverage_id == "gc1")
        self.assertEqual(mirror.mirror_of, "edgeX")
        # 镜像不产生独立片段：只有 edgeX/edgeY 与后续离线片段。
        self.assertNotIn("gateX", {f.source for f in coverage.fragments})

    def test_sample_delivery_does_not_cover_gap(self):
        gaps = self.artifacts[2].coverage.gaps("east", (0, 35))
        self.assertIn((20, 25), gaps)

    def test_complete_offline_supersedes_claimed_coverage(self):
        coverage = self.artifacts[3].coverage
        self.assertIn("ec1", coverage.revoked)
        self.assertEqual(coverage.gaps("east", (0, 35)), [])


class GenerationContinuityTests(PipelineTestBase):
    def test_reset_baseline_is_unassigned(self):
        unassigned = {u.reading_id: u for u in self.artifacts[1].histograms.unassigned}
        self.assertIn("u-a3", unassigned)
        reading = unassigned["u-a3"]
        self.assertEqual(reading.candidate_span, (20, 25))
        self.assertEqual(reading.total, 5)

    def test_unassigned_never_enters_authoritative_total(self):
        k2 = self.fact(1, "k2")
        self.assertEqual(k2.exact_total, 8)  # 仅 p2 差分，复位基线 5 不计
        self.assertEqual(k2.unassigned_total, 5)

    def test_unassigned_resolved_after_offline_completion(self):
        for policy_id in ("k1", "k2"):
            fact = self.fact(3, policy_id)
            self.assertEqual(fact.unassigned_total, 0)

    def test_no_monotonicity_violations_in_sample(self):
        for art in self.artifacts.values():
            self.assertEqual(art.histograms.monotonicity_violations, [])


class RequestDedupTests(PipelineTestBase):
    def test_request_key_dedup_keeps_latest(self):
        model = self.artifacts[3].requests
        # req01/req04/req07 在 sample 与 complete 中重复，complete 晚到胜出。
        keys = {(o.shard, o.request_key) for o in model.observations}
        self.assertEqual(len(keys), len(model.observations))
        req01 = next(o for o in model.observations if o.request_key == "req01")
        self.assertEqual(req01.status, "complete")
        self.assertIn("e-x1", model.superseded_records)

    def test_sample_requests_not_in_authoritative_total(self):
        # v2 的 x-east 是 sample，其请求不能让 east 缺口闭合。
        k2 = self.fact(2, "k2")
        self.assertTrue(k2.uncovered)
        # 但采样慢请求 req04=51 仍抬高慢请求下界。
        self.assertGreaterEqual(k2.slow_lower, 1)


class WindowFactTests(PipelineTestBase):
    def test_v1_window_verdicts(self):
        expected = {
            "k1": Verdict.BREACH,
            "k2": Verdict.INSUFFICIENT,
            "k3": Verdict.BREACH,
            "k4": Verdict.BREACH,
            "k5": Verdict.BREACH,
            "k6": Verdict.NO_SERVICE,
        }
        for policy_id, verdict in expected.items():
            self.assertEqual(self.fact(1, policy_id).verdict, verdict)

    def test_open_gap_forbids_deterministic_verdict_even_if_math_breaches(self):
        k2 = self.fact(1, "k2")
        self.assertIsNotNone(k2.uncovered)
        self.assertEqual(k2.verdict, Verdict.INSUFFICIENT)
        self.assertIsNone(k2.total_upper)  # 开放缺口：总量上界不可知

    def test_cross_window_interval_is_upper_bound_only(self):
        k1 = self.fact(1, "k1")
        kinds = {line.kind for line in k1.evidence}
        self.assertIn(EvidenceKind.HISTOGRAM_PARTIAL, kinds)
        self.assertEqual(k1.exact_total, 15)
        self.assertGreater(k1.total_upper, k1.exact_total)
        self.assertEqual(k1.slow_lower, 8)  # 部分段不贡献慢下界

    def test_offline_replaces_histogram_without_double_count(self):
        k4_v1 = self.fact(1, "k4")
        k4_v2 = self.fact(2, "k4")
        self.assertEqual(k4_v1.exact_total, 8)
        self.assertEqual(k4_v2.exact_total, 5)      # 全量离线接管
        self.assertEqual((k4_v2.slow_lower, k4_v2.slow_upper), (1, 1))
        self.assertEqual(k4_v2.verdict, Verdict.COMPLIANT)
        # 替换区不再有该流直方图权威证据。
        self.assertFalse(
            any(
                line.kind == EvidenceKind.HISTOGRAM
                for line in k4_v2.evidence
            )
        )

    def test_v3_fully_reconciled_east_breach(self):
        k2 = self.fact(3, "k2")
        self.assertEqual(k2.verdict, Verdict.BREACH)
        self.assertEqual(k2.exact_total, 13)
        self.assertEqual((k2.slow_lower, k2.slow_upper), (7, 9))
        self.assertEqual(k2.uncovered, ())

    def test_no_service_window_excluded_from_rate(self):
        k6 = self.fact(1, "k6")
        self.assertEqual(k6.verdict, Verdict.NO_SERVICE)
        self.assertIsNone(k6.rate_lower_ppm)
        self.assertIsNone(k6.rate_upper_ppm)

    def test_every_conclusion_has_traceable_evidence(self):
        for art in self.artifacts.values():
            for fact in art.facts:
                self.assertTrue(fact.evidence, f"{fact.policy_id} 缺少证据行")
                for line in fact.evidence:
                    self.assertTrue(line.source_id)
                    self.assertTrue(line.detail)


class RollupTests(PipelineTestBase):
    def test_pending_campus_keeps_independent_windows(self):
        rollup = self.artifacts[1].rollup
        self.assertFalse(rollup.reconciled)
        self.assertIsNone(rollup.overall_verdict)
        self.assertIn("east", rollup.pending_shards)
        # west 独立可确认，不被 east 牵连。
        self.assertEqual(
            set(rollup.breached_windows) & {"k3", "k4", "k5"},
            {"k3", "k4", "k5"},
        )

    def test_reconciled_campus_gives_formal_verdict(self):
        rollup = self.artifacts[3].rollup
        self.assertTrue(rollup.reconciled)
        self.assertEqual(rollup.overall_verdict, Verdict.BREACH)
        self.assertEqual(rollup.pending_shards, ())

    def test_no_service_distinguished_from_unreconciled(self):
        west = next(s for s in self.artifacts[1].rollup.shards if s.shard == "west")
        self.assertIn("k6", west.no_service_windows)
        self.assertTrue(west.reconciled)


class EventLifecycleTests(PipelineTestBase):
    def test_contiguous_breaches_open_single_event(self):
        ledger = self.artifacts[1].event_ledger
        west_open = [
            p for p in ledger.proposals
            if p.shard == "west" and p.action == ProposalAction.OPEN
        ]
        self.assertEqual(len(west_open), 1)
        self.assertEqual(west_open[0].policies, ("k3", "k4", "k5"))

    def test_amend_preserves_surviving_windows(self):
        ledger = self.artifacts[2].event_ledger
        amend = next(
            p for p in ledger.proposals
            if p.action == ProposalAction.AMEND and p.shard == "west"
        )
        self.assertEqual(amend.policies, ("k3", "k5"))
        self.assertNotIn("k4", amend.policies)

    def test_proposal_versions_are_stable_across_rebuild(self):
        v3 = self.artifacts[3].event_ledger
        v1_open = next(p for p in v3.proposals if p.shard == "west" and p.action == ProposalAction.OPEN)
        self.assertEqual(v1_open.version, 1)
        self.assertEqual(v1_open.state, ProposalState.SUPERSEDED)
        k2_open = next(p for p in v3.proposals if tuple(p.policies) == ("k2",))
        self.assertEqual(k2_open.version, 3)
        self.assertEqual(k2_open.state, ProposalState.ACTIVE)

    def test_insufficient_evidence_only_requests_more_evidence(self):
        self.assertIn("k2", self.artifacts[1].event_ledger.evidence_gap_policies)
        self.assertIn("k2", self.artifacts[2].event_ledger.evidence_gap_policies)
        self.assertNotIn("k2", self.artifacts[3].event_ledger.evidence_gap_policies)
        for version in (1, 2):
            self.assertFalse(
                any(
                    "k2" in p.policies
                    for p in self.artifacts[version].event_ledger.proposals
                )
            )

    def test_different_customers_never_merge_into_one_event(self):
        groups = {g.group_key for g in self.artifacts[3].event_ledger.groups}
        self.assertIn("client1|defect|east", groups)
        self.assertIn("client2|defect|east", groups)


class ReceiptReconciliationTests(PipelineTestBase):
    def test_stale_close_cannot_close_new_gap(self):
        receipts = {r.receipt_id: r for r in self.artifacts[3].event_ledger.receipts}
        self.assertFalse(receipts["ack1"].matches_current_evidence)
        self.assertIsNotNone(receipts["ack1"].stale_reason)
        self.assertTrue(receipts["ack2"].matches_current_evidence)

    def test_human_response_independently_preserved(self):
        receipts = self.artifacts[3].event_ledger.receipts
        human = next(r for r in receipts if r.receipt_id == "human1")
        self.assertEqual(human.action, "response")
        self.assertEqual(human.response, "dispatched")
        inc17 = next(
            l for l in self.artifacts[3].event_ledger.incident_ledgers
            if l.incident == "inc17"
        )
        self.assertTrue(inc17.human_responses)
        self.assertTrue(inc17.platform_state_stale)

    def test_receipt_must_reference_known_submission(self):
        for art in self.artifacts.values():
            for receipt in art.event_ledger.receipts:
                self.assertTrue(receipt.submission_known)


class CredentialStabilityTests(PipelineTestBase):
    def test_credential_deterministic_and_version_distinct(self):
        rebuilt = {a.bundle.version: a for a in build_all_versions(SOURCES)}
        for version, art in self.artifacts.items():
            current = {f.policy_id: f.credential_id for f in art.facts}
            again = {f.policy_id: f.credential_id for f in rebuilt[version].facts}
            self.assertEqual(current, again)
        self.assertNotEqual(
            self.fact(1, "k3").credential_id,
            self.fact(3, "k3").credential_id,
        )


class VersionIsolationTests(unittest.TestCase):
    def test_three_versions_available(self):
        versions = [b.version for b in iter_versions(SOURCES)]
        self.assertEqual(versions, [1, 2, 3])

    def test_single_version_build_is_self_consistent(self):
        bundle = load_version(SOURCES, 2)
        artifacts = build_version_artifacts(bundle)
        self.assertEqual(len(artifacts.facts), 6)
        self.assertTrue(all(f.version == 2 for f in artifacts.facts))


if __name__ == "__main__":
    unittest.main()
