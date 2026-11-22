"""SQLite 版本化持久化与服务凭证。

设计要点：

* 所有事实表带 ``version`` 列且只追加：历史版本一旦写入永不改写，回查历史
  与日常整理可以并发进行（WAL 模式，读不阻塞写）；
* ``window_credentials`` 以凭证号为主键，同一 (版本, 窗口, 结论, 界限) 始终
  得到同一凭证号，一份正在使用的凭证不会变成部分新口径、部分旧计数；
* 证据行、未归属读数、事件提案、平台回执全部落库，任一结论可下钻追查；
* 纯标准库实现，数据库文件可随时删除后由 ``rebuild`` 完整重放。
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Optional

from .events import EventLedger, ProposalState
from .intake import SourceBundle
from .model import WindowFact
from .pipeline import build_all_versions
from .rollup import CampusRollup

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS versions (
    version INTEGER PRIMARY KEY,
    intake_no INTEGER NOT NULL,
    built_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS window_facts (
    credential_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    policy_id TEXT NOT NULL,
    customer TEXT NOT NULL,
    service TEXT NOT NULL,
    shard TEXT NOT NULL,
    span_start INTEGER NOT NULL,
    span_end INTEGER NOT NULL,
    threshold INTEGER NOT NULL,
    max_ppm INTEGER NOT NULL,
    exact_total INTEGER,
    total_upper INTEGER,
    slow_lower INTEGER NOT NULL,
    slow_upper INTEGER NOT NULL,
    unassigned_total INTEGER NOT NULL,
    uncovered TEXT NOT NULL,
    verdict TEXT NOT NULL,
    rate_lower_ppm REAL,
    rate_upper_ppm REAL,
    PRIMARY KEY (version, policy_id)
);
CREATE TABLE IF NOT EXISTS evidence_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    policy_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    span_start INTEGER NOT NULL,
    span_end INTEGER NOT NULL,
    exact_total INTEGER,
    total_upper INTEGER,
    slow_lower INTEGER NOT NULL,
    slow_upper INTEGER NOT NULL,
    detail TEXT NOT NULL,
    intake INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS unassigned_readings (
    version INTEGER NOT NULL,
    reading_id TEXT NOT NULL,
    shard TEXT NOT NULL,
    stream TEXT NOT NULL,
    generation TEXT NOT NULL,
    span_start INTEGER NOT NULL,
    span_end INTEGER NOT NULL,
    total INTEGER NOT NULL,
    reason TEXT NOT NULL,
    intake INTEGER NOT NULL,
    PRIMARY KEY (version, reading_id)
);
CREATE TABLE IF NOT EXISTS shard_summaries (
    version INTEGER NOT NULL,
    shard TEXT NOT NULL,
    exact_total INTEGER NOT NULL,
    total_upper INTEGER,
    slow_lower INTEGER NOT NULL,
    slow_upper INTEGER NOT NULL,
    unassigned_total INTEGER NOT NULL,
    no_service_windows TEXT NOT NULL,
    breached_windows TEXT NOT NULL,
    compliant_windows TEXT NOT NULL,
    insufficient_windows TEXT NOT NULL,
    unreconciled_spans TEXT NOT NULL,
    reconciled INTEGER NOT NULL,
    PRIMARY KEY (version, shard)
);
CREATE TABLE IF NOT EXISTS campus_rollups (
    version INTEGER PRIMARY KEY,
    exact_total INTEGER NOT NULL,
    total_upper INTEGER,
    slow_lower INTEGER NOT NULL,
    slow_upper INTEGER NOT NULL,
    unassigned_total INTEGER NOT NULL,
    breached_windows TEXT NOT NULL,
    compliant_windows TEXT NOT NULL,
    insufficient_windows TEXT NOT NULL,
    no_service_windows TEXT NOT NULL,
    reconciled INTEGER NOT NULL,
    overall_verdict TEXT,
    pending_shards TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_proposals (
    proposal_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    group_key TEXT NOT NULL,
    customer TEXT NOT NULL,
    service TEXT NOT NULL,
    shard TEXT NOT NULL,
    action TEXT NOT NULL,
    policies TEXT NOT NULL,
    credential_ids TEXT NOT NULL,
    basis TEXT NOT NULL,
    state TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS platform_submissions (
    submission_id TEXT PRIMARY KEY,
    customer TEXT NOT NULL,
    service TEXT NOT NULL,
    shard TEXT NOT NULL,
    policies TEXT NOT NULL,
    intake INTEGER NOT NULL,
    action TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS platform_receipts (
    version INTEGER NOT NULL,
    receipt_id TEXT NOT NULL,
    submission_id TEXT NOT NULL,
    incident TEXT NOT NULL,
    action TEXT NOT NULL,
    response TEXT,
    intake INTEGER NOT NULL,
    submission_known INTEGER NOT NULL,
    matches_current_evidence INTEGER NOT NULL,
    stale_reason TEXT,
    PRIMARY KEY (version, receipt_id)
);
CREATE TABLE IF NOT EXISTS evidence_gap_requests (
    version INTEGER NOT NULL,
    policy_id TEXT NOT NULL,
    PRIMARY KEY (version, policy_id)
);
"""


class _StateRow:
    """对 sqlite3.Row 的只读包装，覆盖 state 列为查询版本下的有效状态。"""

    def __init__(self, row: sqlite3.Row, effective_state: str):
        self._row = row
        self._state = effective_state

    def __getitem__(self, key):
        if key == "state":
            return self._state
        return self._row[key]

    def keys(self):
        return self._row.keys()

    def get(self, key, default=None):
        return self[key] if key in self._row.keys() else default


class FactStore:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', '1')"
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "FactStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------- 写入
    def persist_version(
        self,
        bundle: SourceBundle,
        facts: list[WindowFact],
        rollup: CampusRollup,
        ledger: EventLedger,
        unassigned: Optional[list] = None,
    ) -> None:
        version = bundle.version
        if self.conn.execute(
            "SELECT 1 FROM versions WHERE version=?", (version,)
        ).fetchone():
            return  # 幂等：已写入的版本不重复追加

        cur = self.conn
        intake_no = max(d.intake for d in bundle.deliveries)
        cur.execute(
            "INSERT INTO versions(version, intake_no) VALUES (?, ?)",
            (version, intake_no),
        )
        for fact in facts:
            cur.execute(
                """INSERT INTO window_facts VALUES (
                    :credential_id,:version,:policy_id,:customer,:service,:shard,
                    :span_start,:span_end,:threshold,:max_ppm,:exact_total,
                    :total_upper,:slow_lower,:slow_upper,:unassigned_total,
                    :uncovered,:verdict,:rate_lower_ppm,:rate_upper_ppm)""",
                dict(
                    credential_id=fact.credential_id,
                    version=version,
                    policy_id=fact.policy_id,
                    customer=fact.customer,
                    service=fact.service,
                    shard=fact.shard,
                    span_start=fact.span[0],
                    span_end=fact.span[1],
                    threshold=fact.threshold,
                    max_ppm=fact.max_ppm,
                    exact_total=fact.exact_total,
                    total_upper=fact.total_upper,
                    slow_lower=fact.slow_lower,
                    slow_upper=fact.slow_upper,
                    unassigned_total=fact.unassigned_total,
                    uncovered=json.dumps(list(fact.uncovered)),
                    verdict=fact.verdict.value,
                    rate_lower_ppm=fact.rate_lower_ppm,
                    rate_upper_ppm=fact.rate_upper_ppm,
                ),
            )
            for line in fact.evidence:
                cur.execute(
                    """INSERT INTO evidence_lines(
                        version,policy_id,kind,source_id,span_start,span_end,
                        exact_total,total_upper,slow_lower,slow_upper,detail,intake)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        version,
                        fact.policy_id,
                        line.kind.value,
                        line.source_id,
                        line.span[0],
                        line.span[1],
                        line.exact_total,
                        line.total_upper,
                        line.slow_lower,
                        line.slow_upper,
                        line.detail,
                        line.intake,
                    ),
                )

        for reading in unassigned or []:
            self.conn.execute(
                """INSERT OR IGNORE INTO unassigned_readings VALUES (
                    :version,:reading_id,:shard,:stream,:generation,
                    :span_start,:span_end,:total,:reason,:intake)""",
                dict(
                    version=version,
                    reading_id=reading.reading_id,
                    shard=reading.shard,
                    stream=reading.stream,
                    generation=reading.generation,
                    span_start=reading.candidate_span[0],
                    span_end=reading.candidate_span[1],
                    total=reading.total,
                    reason=reading.reason,
                    intake=reading.intake,
                ),
            )

        for summary in rollup.shards:
            cur.execute(
                """INSERT INTO shard_summaries VALUES (
                    :version,:shard,:exact_total,:total_upper,:slow_lower,
                    :slow_upper,:unassigned_total,:no_service_windows,
                    :breached_windows,:compliant_windows,:insufficient_windows,
                    :unreconciled_spans,:reconciled)""",
                dict(
                    version=version,
                    shard=summary.shard,
                    exact_total=summary.exact_total,
                    total_upper=summary.total_upper,
                    slow_lower=summary.slow_lower,
                    slow_upper=summary.slow_upper,
                    unassigned_total=summary.unassigned_total,
                    no_service_windows=json.dumps(list(summary.no_service_windows)),
                    breached_windows=json.dumps(list(summary.breached_windows)),
                    compliant_windows=json.dumps(list(summary.compliant_windows)),
                    insufficient_windows=json.dumps(
                        list(summary.insufficient_windows)
                    ),
                    unreconciled_spans=json.dumps(list(summary.unreconciled_spans)),
                    reconciled=int(summary.reconciled),
                ),
            )

        cur.execute(
            """INSERT INTO campus_rollups VALUES (
                :version,:exact_total,:total_upper,:slow_lower,:slow_upper,
                :unassigned_total,:breached_windows,:compliant_windows,
                :insufficient_windows,:no_service_windows,:reconciled,
                :overall_verdict,:pending_shards)""",
            dict(
                version=version,
                exact_total=rollup.exact_total,
                total_upper=rollup.total_upper,
                slow_lower=rollup.slow_lower,
                slow_upper=rollup.slow_upper,
                unassigned_total=rollup.unassigned_total,
                breached_windows=json.dumps(list(rollup.breached_windows)),
                compliant_windows=json.dumps(list(rollup.compliant_windows)),
                insufficient_windows=json.dumps(list(rollup.insufficient_windows)),
                no_service_windows=json.dumps(list(rollup.no_service_windows)),
                reconciled=int(rollup.reconciled),
                overall_verdict=(
                    rollup.overall_verdict.value
                    if rollup.overall_verdict
                    else None
                ),
                pending_shards=json.dumps(list(rollup.pending_shards)),
            ),
        )

        for proposal in ledger.proposals:
            # 提案带其真实产生版本。最终版本的切片包含完整提案历史，统一
            # 落库一次；历史版本切片只含本版提案，由 INSERT OR IGNORE 兜底。
            cur.execute(
                """INSERT OR IGNORE INTO event_proposals VALUES (
                    :proposal_id,:version,:group_key,:customer,:service,:shard,
                    :action,:policies,:credential_ids,:basis,:state)""",
                dict(
                    proposal_id=proposal.proposal_id,
                    version=proposal.version,
                    group_key=proposal.group_key,
                    customer=proposal.customer,
                    service=proposal.service,
                    shard=proposal.shard,
                    action=proposal.action.value,
                    policies=json.dumps(list(proposal.policies)),
                    credential_ids=json.dumps(list(proposal.credential_ids)),
                    basis=proposal.basis,
                    state=proposal.state.value,
                ),
            )

        for submission in ledger.submissions:
            cur.execute(
                """INSERT OR IGNORE INTO platform_submissions VALUES (
                    :submission_id,:customer,:service,:shard,:policies,:intake,:action)""",
                dict(
                    submission_id=submission.submission_id,
                    customer=submission.customer,
                    service=submission.service,
                    shard=submission.shard,
                    policies=json.dumps(list(submission.policies)),
                    intake=submission.intake,
                    action=submission.action,
                ),
            )
        for receipt in ledger.receipts:
            cur.execute(
                """INSERT OR IGNORE INTO platform_receipts VALUES (
                    :version,:receipt_id,:submission_id,:incident,:action,:response,
                    :intake,:submission_known,:matches_current_evidence,:stale_reason)""",
                dict(
                    version=version,
                    receipt_id=receipt.receipt_id,
                    submission_id=receipt.submission_id,
                    incident=receipt.incident,
                    action=receipt.action,
                    response=receipt.response,
                    intake=receipt.intake,
                    submission_known=int(receipt.submission_known),
                    matches_current_evidence=int(receipt.matches_current_evidence),
                    stale_reason=receipt.stale_reason,
                ),
            )
        for policy_id in ledger.evidence_gap_policies:
            cur.execute(
                "INSERT OR IGNORE INTO evidence_gap_requests VALUES (?, ?)",
                (version, policy_id),
            )
        self.conn.commit()

    # ------------------------------------------------------------- 查询
    def latest_version(self) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MAX(version) AS v FROM versions"
        ).fetchone()
        return row["v"]

    def query_windows(
        self,
        version: Optional[int] = None,
        customer: Optional[str] = None,
        service: Optional[str] = None,
        shard: Optional[str] = None,
    ) -> list[sqlite3.Row]:
        version = version or self.latest_version()
        sql = "SELECT * FROM window_facts WHERE version=?"
        params: list = [version]
        for column, value in (
            ("customer", customer),
            ("service", service),
            ("shard", shard),
        ):
            if value is not None:
                sql += f" AND {column}=?"
                params.append(value)
        sql += " ORDER BY shard, span_start"
        return list(self.conn.execute(sql, params))

    def query_evidence(self, version: int, policy_id: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM evidence_lines WHERE version=? AND policy_id=? "
                "ORDER BY span_start, id",
                (version, policy_id),
            )
        )

    def query_rollup(self, version: Optional[int] = None) -> sqlite3.Row:
        version = version or self.latest_version()
        return self.conn.execute(
            "SELECT * FROM campus_rollups WHERE version=?", (version,)
        ).fetchone()

    def query_shards(self, version: Optional[int] = None) -> list[sqlite3.Row]:
        version = version or self.latest_version()
        return list(
            self.conn.execute(
                "SELECT * FROM shard_summaries WHERE version=? ORDER BY shard",
                (version,),
            )
        )

    def query_proposals(
        self,
        version: Optional[int] = None,
        state: Optional[ProposalState] = None,
        only_version: bool = False,
    ) -> list[sqlite3.Row]:
        """事件提案。默认返回截至该版本的完整提案历史（含订正链）。

        状态按查询版本动态计算：同组若存在更新提案，则较早提案在该版本视角
        下为 ``superseded``，与流水线切片口径一致。``only_version=True`` 时
        只返回该版本新产生的提案。
        """
        version = version or self.latest_version()
        comparator = "=" if only_version else "<="
        rows = list(
            self.conn.execute(
                f"SELECT * FROM event_proposals WHERE version {comparator} ? "
                "ORDER BY version, proposal_id",
                (version,),
            )
        )
        # 每组在查询版本内的最大提案版本，早于它的同组提案即被取代。
        latest_version_per_group: dict[str, int] = {}
        for r in rows:
            latest_version_per_group[r["group_key"]] = max(
                latest_version_per_group.get(r["group_key"], -1), r["version"]
            )
        enriched = []
        for r in rows:
            effective_state = (
                ProposalState.SUPERSEDED.value
                if r["version"] < latest_version_per_group[r["group_key"]]
                else ProposalState.ACTIVE.value
            )
            if state is not None and effective_state != state.value:
                continue
            enriched.append(_StateRow(r, effective_state))
        return enriched

    def query_receipts(self, version: Optional[int] = None) -> list[sqlite3.Row]:
        version = version or self.latest_version()
        return list(
            self.conn.execute(
                "SELECT * FROM platform_receipts WHERE version=? ORDER BY intake, receipt_id",
                (version,),
            )
        )

    def query_unassigned(self, version: Optional[int] = None) -> list[sqlite3.Row]:
        version = version or self.latest_version()
        return list(
            self.conn.execute(
                "SELECT * FROM unassigned_readings WHERE version=? ORDER BY shard, reading_id",
                (version,),
            ),
        )

    def versions(self) -> list[int]:
        return [
            row["version"]
            for row in self.conn.execute(
                "SELECT version FROM versions ORDER BY version"
            )
        ]


def rebuild(source_dir: str | Path, db_path: str | Path) -> FactStore:
    """从原件完整重放所有版本（删除旧库后重建）。

    事件台账依赖完整版本序列，因此先用流水线一次性推演出各版本产物（窗口
    事实按版本独立、事件提案共享同一条订正链），再逐版落库。
    """
    path = Path(db_path)
    for suffix in ("", "-wal", "-shm"):
        target = Path(str(path) + suffix)
        if target.exists():
            target.unlink()
    store = FactStore(path)
    for artifacts in build_all_versions(Path(source_dir)):
        store.persist_version(
            artifacts.bundle,
            artifacts.facts,
            artifacts.rollup,
            artifacts.event_ledger,
            unassigned=artifacts.histograms.unassigned,
        )
    return store
