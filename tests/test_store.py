"""SQLite 版本化存储与服务凭证测试。"""
import sqlite3
import tempfile
import unittest
from pathlib import Path

from latency_fact.model import Verdict
from latency_fact.pipeline import build_all_versions
from latency_fact.store import FactStore, rebuild

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ROOT / "sources"


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "fact.db"
        self.artifacts = {a.bundle.version: a for a in build_all_versions(SOURCES)}
        self.store = rebuild(SOURCES, self.db_path)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_all_versions_persisted_append_only(self):
        self.assertEqual(self.store.versions(), [1, 2, 3])
        rows_v1 = self.store.query_windows(version=1)
        rows_v3 = self.store.query_windows(version=3)
        self.assertEqual(len(rows_v1), 6)
        self.assertEqual(len(rows_v3), 6)
        # 重放不重复追加。
        rebuild(SOURCES, self.db_path).close()
        again = FactStore(self.db_path)
        self.assertEqual(again.versions(), [1, 2, 3])
        again.close()

    def test_credential_id_persisted_and_stable(self):
        rows = {r["policy_id"]: r for r in self.store.query_windows(version=3)}
        expected = {f.policy_id: f.credential_id for f in self.artifacts[3].facts}
        for policy_id, credential_id in expected.items():
            self.assertEqual(rows[policy_id]["credential_id"], credential_id)

    def test_evidence_drilldown(self):
        rows = self.store.query_evidence(3, "k4")
        self.assertTrue(rows)
        self.assertTrue(any("x-west" in r["source_id"] for r in rows))
        # 每个证据行都有人类可读的追查说明。
        self.assertTrue(all(r["detail"] for r in rows))

    def test_unassigned_readings_kept_for_audit(self):
        v1 = self.store.query_unassigned(1)
        self.assertTrue(any(r["reading_id"] == "u-a3" for r in v1))
        v3 = self.store.query_unassigned(3)
        # 读数作为审计痕迹在各版本保留。
        self.assertTrue(any(r["reading_id"] == "u-a3" for r in v3))

    def test_proposals_and_receipts_queryable(self):
        # 提案按版本留存：amend 产生于 v2，v3 当版只有新 open。
        all_proposals = []
        for version in self.store.versions():
            all_proposals.extend(self.store.query_proposals(version))
        self.assertTrue(any(p["action"] == "amend" for p in all_proposals))
        v3_proposals = self.store.query_proposals(3)
        self.assertTrue(
            any(p["policies"] == '["k2"]' for p in v3_proposals)
        )
        receipts = self.store.query_receipts(3)
        ack1 = next(r for r in receipts if r["receipt_id"] == "ack1")
        self.assertEqual(ack1["matches_current_evidence"], 0)
        self.assertIsNotNone(ack1["stale_reason"])

    def test_rollup_pending_flag_persisted(self):
        v1 = self.store.query_rollup(1)
        self.assertEqual(v1["reconciled"], 0)
        self.assertIsNone(v1["overall_verdict"])
        v3 = self.store.query_rollup(3)
        self.assertEqual(v3["reconciled"], 1)
        self.assertEqual(v3["overall_verdict"], Verdict.BREACH.value)

    def test_wal_mode_enables_concurrent_reader(self):
        # 写者保持连接时，独立只读连接仍可查询（WAL 不阻塞）。
        mode = self.store.conn.execute("PRAGMA journal_mode").fetchone()[0]
        self.assertEqual(mode.lower(), "wal")
        reader = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            count = reader.execute(
                "SELECT COUNT(*) FROM window_facts WHERE version=2"
            ).fetchone()[0]
            self.assertEqual(count, 6)
        finally:
            reader.close()

    def test_reader_survives_open_writer_transaction(self):
        # 日常整理（写）与历史回查（读）并发：写事务未提交时，读者仍能读到
        # 已提交的旧版本，不会被阻塞或读到半成品。
        self.store.conn.execute("BEGIN")
        self.store.conn.execute(
            "CREATE TABLE IF NOT EXISTS scratch(x INTEGER)"
        )
        reader = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            versions = [r[0] for r in reader.execute(
                "SELECT DISTINCT version FROM versions ORDER BY version"
            )]
            self.assertEqual(versions, [1, 2, 3])
            # 未提交的草稿表对读者不可见。
            with self.assertRaises(sqlite3.OperationalError):
                reader.execute("SELECT COUNT(*) FROM scratch")
        finally:
            reader.close()
            self.store.conn.rollback()

    def test_rebuild_is_deterministic(self):
        first = {
            (r["version"], r["policy_id"]): r["credential_id"]
            for v in (1, 2, 3) for r in self.store.query_windows(v)
        }
        self.store.close()
        second_store = rebuild(SOURCES, self.db_path)
        second = {
            (r["version"], r["policy_id"]): r["credential_id"]
            for v in (1, 2, 3) for r in second_store.query_windows(v)
        }
        self.assertEqual(first, second)
        second_store.close()


if __name__ == "__main__":
    unittest.main()
