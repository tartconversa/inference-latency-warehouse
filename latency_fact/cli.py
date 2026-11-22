"""本地命令行入口。

示例
====
.. code-block:: sh

    python -m latency_fact.cli rebuild                 # 从原件重放全部版本
    python -m latency_fact.cli windows --version 1    # 查 v1 窗口事实
    python -m latency_fact.cli evidence --policy k4   # 追查 k4 全部证据行
    python -m latency_fact.cli rollup                  # 园区汇总
    python -m latency_fact.cli events                  # 事件提案与订正链
    python -m latency_fact.cli receipts                # 回执对账
    python -m latency_fact.cli verify                  # 内置不变量验证
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .model import Verdict
from .store import FactStore, rebuild

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "latency_fact.db"
DEFAULT_SOURCES = ROOT / "sources"

VERDICT_TEXT = {
    Verdict.BREACH.value: "确实超标",
    Verdict.COMPLIANT.value: "确实符合",
    Verdict.INSUFFICIENT.value: "证据不足",
    Verdict.NO_SERVICE.value: "确证无服务",
}


def _store(args: argparse.Namespace) -> FactStore:
    return FactStore(args.db)


def cmd_rebuild(args: argparse.Namespace) -> int:
    store = rebuild(args.sources, args.db)
    versions = store.versions()
    print(f"已重建 {args.db}：版本 {versions}")
    store.close()
    return 0


def _fmt_ppm(value: object) -> str:
    return "—" if value is None else f"{float(value):,.0f} ppm"


def cmd_windows(args: argparse.Namespace) -> int:
    with _store(args) as store:
        rows = store.query_windows(
            version=args.version,
            customer=args.customer,
            service=args.service,
            shard=args.shard,
        )
        header = (
            f"{'版本':>2} {'窗口':<4} {'客户':<8} {'站点':<5} {'区间':<9} "
            f"{'权威量':>5} {'总量上界':>6} {'慢下界':>5} {'慢上界':>5} "
            f"{'未归属':>5} {'费率区间':<20} {'判定'}"
        )
        print(header)
        for row in rows:
            rate = f"{_fmt_ppm(row['rate_lower_ppm'])} ~ {_fmt_ppm(row['rate_upper_ppm'])}"
            upper = "开放" if row["total_upper"] is None else str(row["total_upper"])
            print(
                f"{row['version']:>2} {row['policy_id']:<4} {row['customer']:<8} "
                f"{row['shard']:<5} [{row['span_start']:>2},{row['span_end']:>2})  "
                f"{str(row['exact_total']):>5} {upper:>6} {row['slow_lower']:>5} "
                f"{row['slow_upper']:>5} {row['unassigned_total']:>5} "
                f"{rate:<20} {VERDICT_TEXT[row['verdict']]}"
            )
            if row["uncovered"] != "[]":
                print(f"     └ 未覆盖缺口: {row['uncovered']}")
    return 0


def cmd_evidence(args: argparse.Namespace) -> int:
    with _store(args) as store:
        version = args.version or store.latest_version()
        rows = store.query_evidence(version, args.policy)
        if not rows:
            print(f"版本 {version} 窗口 {args.policy} 无证据行")
            return 1
        print(f"窗口 {args.policy} @ v{version} 的证据链：")
        for row in rows:
            exact = "—" if row["exact_total"] is None else row["exact_total"]
            print(
                f"  [{row['kind']:<17}] {row['source_id']:<14} "
                f"[{row['span_start']:>2},{row['span_end']:>2}) "
                f"权威量={exact} 上界={row['total_upper']} "
                f"慢=[{row['slow_lower']},{row['slow_upper']}] "
                f"批次={row['intake']} | {row['detail']}"
            )
    return 0


def cmd_rollup(args: argparse.Namespace) -> int:
    with _store(args) as store:
        version = args.version or store.latest_version()
        print(f"园区汇总 @ v{version}")
        for shard in store.query_shards(version):
            state = "已核清" if shard["reconciled"] else "未核清"
            print(
                f"  站点 {shard['shard']:<5} {state} 权威请求={shard['exact_total']} "
                f"慢=[{shard['slow_lower']},{shard['slow_upper']}] "
                f"未归属={shard['unassigned_total']}"
            )
            if shard["unreconciled_spans"] != "[]":
                print(f"    未能核清时段: {shard['unreconciled_spans']}")
            if shard["no_service_windows"] != "[]":
                print(f"    确证无服务窗口: {shard['no_service_windows']}")
        campus = store.query_rollup(version)
        overall = (
            VERDICT_TEXT[campus["overall_verdict"]]
            if campus["overall_verdict"]
            else f"正式结论保留（待核清站点 {campus['pending_shards']}）"
        )
        print(
            f"  园区: 权威请求={campus['exact_total']} "
            f"慢=[{campus['slow_lower']},{campus['slow_upper']}] "
            f"超标窗口={campus['breached_windows']} 总体判定={overall}"
        )
    return 0


def cmd_events(args: argparse.Namespace) -> int:
    with _store(args) as store:
        version = args.version or store.latest_version()
        print(f"事件提案 @ v{version}")
        for row in store.query_proposals(version):
            print(
                f"  [{row['action']:<8}] {row['proposal_id']} 状态={row['state']} "
                f"窗口={row['policies']} 凭证={row['credential_ids']}"
            )
            print(f"      依据: {row['basis']}")
        gaps = list(
            store.conn.execute(
                "SELECT policy_id FROM evidence_gap_requests WHERE version=? ORDER BY policy_id",
                (version,),
            )
        )
        if gaps:
            print(f"  补取证请求（证据不足，未建事件）: {[r['policy_id'] for r in gaps]}")
    return 0


def cmd_receipts(args: argparse.Namespace) -> int:
    with _store(args) as store:
        version = args.version or store.latest_version()
        print(f"平台回执对账 @ v{version}")
        for row in store.query_receipts(version):
            flag = "采信" if row["matches_current_evidence"] else "陈旧/失效"
            print(
                f"  {row['receipt_id']:<7} 提交={row['submission_id']:<9} "
                f"事件={row['incident']} 动作={row['action']:<9} {flag}"
            )
            if row["stale_reason"]:
                print(f"      原因: {row['stale_reason']}")
            if row["response"]:
                print(f"      人工响应（独立保留）: {row['response']}")
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    """内置不变量验证：无需外部框架，失败即非零退出。"""
    from .verify import run_checks

    failures = run_checks(args.sources)
    if failures:
        for failure in failures:
            print(f"FAIL {failure}", file=sys.stderr)
        print(f"验证失败：{len(failures)} 项", file=sys.stderr)
        return 1
    print("全部不变量验证通过")
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """把某版本窗口事实导出为 JSON，便于外部平台取数。"""
    with _store(args) as store:
        version = args.version or store.latest_version()
        rows = [dict(r) for r in store.query_windows(version)]
        print(json.dumps({"version": version, "window_facts": rows}, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="边缘推理延时事实仓")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 库路径")
    parser.add_argument("--sources", default=str(DEFAULT_SOURCES), help="源资料目录")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("rebuild", help="从原件重放全部版本").set_defaults(func=cmd_rebuild)

    p_win = sub.add_parser("windows", help="查询窗口事实")
    p_win.add_argument("--version", type=int)
    p_win.add_argument("--customer")
    p_win.add_argument("--service")
    p_win.add_argument("--shard")
    p_win.set_defaults(func=cmd_windows)

    p_ev = sub.add_parser("evidence", help="追查窗口证据链")
    p_ev.add_argument("--policy", required=True)
    p_ev.add_argument("--version", type=int)
    p_ev.set_defaults(func=cmd_evidence)

    p_roll = sub.add_parser("rollup", help="站点/园区汇总")
    p_roll.add_argument("--version", type=int)
    p_roll.set_defaults(func=cmd_rollup)

    p_evt = sub.add_parser("events", help="事件提案与订正链")
    p_evt.add_argument("--version", type=int)
    p_evt.set_defaults(func=cmd_events)

    p_rec = sub.add_parser("receipts", help="回执对账")
    p_rec.add_argument("--version", type=int)
    p_rec.set_defaults(func=cmd_receipts)

    sub.add_parser("verify", help="内置不变量验证").set_defaults(func=cmd_verify)

    p_exp = sub.add_parser("export", help="导出窗口事实 JSON")
    p_exp.add_argument("--version", type=int)
    p_exp.set_defaults(func=cmd_export)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
