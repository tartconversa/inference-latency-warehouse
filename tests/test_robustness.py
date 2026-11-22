"""异常与边界输入的健壮性测试。

不修改 sources/ 原件，全部在内存中构造合成 SourceBundle，验证系统对镜像
冲突、累计倒退、跨代桶边界变化等异常的显式暴露能力。
"""
import unittest

from latency_fact.coverage import build_coverage
from latency_fact.histograms import build_histograms
from latency_fact.intake import SourceBundle, Delivery


def bundle(records_by_table: dict[str, list[dict]], intake: int = 1) -> SourceBundle:
    deliveries = tuple(
        Delivery(intake=intake, table=table, records=tuple(rows), path=f"<synth>/<{table}>")
        for table, rows in records_by_table.items()
    )
    return SourceBundle(version=1, deliveries=deliveries)


COVERAGE_BASE = [
    {"id": "c1", "stream": "edge", "shard": "s", "span": [0, 30],
     "grade": "complete", "mirror_of": None},
]


class MirrorConflictTests(unittest.TestCase):
    def test_mirror_count_mismatch_is_flagged(self):
        data = {
            "coverage": COVERAGE_BASE + [
                {"id": "m1", "stream": "gate", "shard": "s", "span": [0, 30],
                 "grade": "complete", "mirror_of": "edge"},
            ],
            "histograms": [
                {"id": "e0", "stream": "edge", "shard": "s", "generation": "g",
                 "time": 0, "bounds": ["20", "inf"], "counts": [0, 0]},
                {"id": "e1", "stream": "edge", "shard": "s", "generation": "g",
                 "time": 10, "bounds": ["20", "inf"], "counts": [3, 4]},
                {"id": "g0", "stream": "gate", "shard": "s", "generation": "h",
                 "time": 0, "bounds": ["20", "inf"], "counts": [0, 0]},
                # 镜像在同一时刻计数与被镜像流不一致。
                {"id": "g1", "stream": "gate", "shard": "s", "generation": "h",
                 "time": 10, "bounds": ["20", "inf"], "counts": [9, 9]},
            ],
        }
        model = build_histograms(bundle(data), build_coverage(bundle(data)))
        self.assertTrue(model.mirror_conflicts)
        # 镜像流本身不产生独立证据。
        self.assertNotIn("gate", {i.stream for i in model.intervals})


class MonotonicityTests(unittest.TestCase):
    def test_cumulative_regression_is_flagged(self):
        data = {
            "coverage": COVERAGE_BASE,
            "histograms": [
                {"id": "e0", "stream": "edge", "shard": "s", "generation": "g",
                 "time": 0, "bounds": ["20", "inf"], "counts": [0, 0]},
                {"id": "e1", "stream": "edge", "shard": "s", "generation": "g",
                 "time": 10, "bounds": ["20", "inf"], "counts": [5, 5]},
                # 同一桶累计倒退。
                {"id": "e2", "stream": "edge", "shard": "s", "generation": "g",
                 "time": 20, "bounds": ["20", "inf"], "counts": [4, 6]},
            ],
        }
        model = build_histograms(bundle(data), build_coverage(bundle(data)))
        self.assertTrue(model.monotonicity_violations)


class GenerationResetTests(unittest.TestCase):
    def test_baseline_after_reset_is_unassigned(self):
        data = {
            "coverage": COVERAGE_BASE,
            "histograms": [
                {"id": "e0", "stream": "edge", "shard": "s", "generation": "g1",
                 "time": 0, "bounds": ["20", "inf"], "counts": [0, 0]},
                {"id": "e1", "stream": "edge", "shard": "s", "generation": "g1",
                 "time": 10, "bounds": ["20", "inf"], "counts": [2, 3]},
                # 新代首端点携带非零累计：复位前来源不可知。
                {"id": "r0", "stream": "edge", "shard": "s", "generation": "g2",
                 "time": 20, "bounds": ["20", "inf"], "counts": [1, 1]},
                {"id": "r1", "stream": "edge", "shard": "s", "generation": "g2",
                 "time": 30, "bounds": ["20", "inf"], "counts": [3, 2]},
            ],
        }
        model = build_histograms(bundle(data), build_coverage(bundle(data)))
        unassigned = {u.reading_id for u in model.unassigned}
        self.assertIn("u-r0", unassigned)
        # g2 代内 (20,30] 的合法差分仍被保留。
        diffs = {i.interval_id for i in model.intervals}
        self.assertIn("h-r0-r1", diffs)

    def test_bucket_boundary_change_blocks_delta(self):
        data = {
            "coverage": COVERAGE_BASE,
            "histograms": [
                {"id": "e0", "stream": "edge", "shard": "s", "generation": "g",
                 "time": 0, "bounds": ["20", "inf"], "counts": [0, 0]},
                # 相邻端点桶边界变化：不能按桶差分，降级未归属。
                {"id": "e1", "stream": "edge", "shard": "s", "generation": "g",
                 "time": 10, "bounds": ["10", "20", "inf"], "counts": [1, 2, 3]},
            ],
        }
        model = build_histograms(bundle(data), build_coverage(bundle(data)))
        self.assertEqual(model.intervals, [])
        self.assertTrue(model.unassigned)


class CoverageOutsideTests(unittest.TestCase):
    def test_interval_outside_coverage_is_unassigned(self):
        data = {
            "coverage": [
                {"id": "c1", "stream": "edge", "shard": "s", "span": [0, 10],
                 "grade": "complete", "mirror_of": None},
            ],
            "histograms": [
                {"id": "e0", "stream": "edge", "shard": "s", "generation": "g",
                 "time": 0, "bounds": ["20", "inf"], "counts": [0, 0]},
                # (10,20] 超出覆盖声明，不能作为权威证据。
                {"id": "e1", "stream": "edge", "shard": "s", "generation": "g",
                 "time": 20, "bounds": ["20", "inf"], "counts": [4, 4]},
            ],
        }
        coverage = build_coverage(bundle(data))
        model = build_histograms(bundle(data), coverage)
        self.assertEqual(model.intervals, [])
        self.assertTrue(
            any("未完整落在本流权威覆盖内" in u.reason for u in model.unassigned)
        )


if __name__ == "__main__":
    unittest.main()
