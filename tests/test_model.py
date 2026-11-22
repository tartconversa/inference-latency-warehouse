"""区间与桶的纯函数测试：半开区间语义与承诺线跨桶界限。"""
import unittest

from latency_fact.model import (
    Bucket,
    intersect,
    merge_spans,
    parse_bounds,
    rate_breaches,
    uncovered_within,
)


class SpanTests(unittest.TestCase):
    def test_intersect_half_open_and_touching(self):
        self.assertEqual(intersect((0, 10), (5, 15)), (5, 10))
        self.assertIsNone(intersect((0, 10), (10, 20)))  # 相接零宽不算相交
        self.assertIsNone(intersect((0, 10), (11, 20)))

    def test_merge_and_gaps(self):
        self.assertEqual(merge_spans([(0, 5), (5, 10), (12, 14)]), [(0, 10), (12, 14)])
        self.assertEqual(
            uncovered_within([(0, 5), (8, 12)], (0, 15)),
            [(5, 8), (12, 15)],
        )

    def test_parse_bounds_with_inf(self):
        self.assertEqual(parse_bounds(["20", "80", "inf"]), (20, 80, float("inf")))


class BucketSlowRangeTests(unittest.TestCase):
    def test_bucket_entirely_slow_or_fast(self):
        fast = Bucket(0, 20)
        line = Bucket(20, 80)
        slow = Bucket(80, None)
        self.assertEqual(fast.slow_range(50, 7), (0, 0))
        self.assertEqual(slow.slow_range(50, 3), (3, 3))

    def test_threshold_crossing_bucket_keeps_range_only(self):
        # 承诺线 50 穿过 (20,80]：禁止均匀假设，只能给 [0, count]。
        crossing = Bucket(20, 80)
        self.assertEqual(crossing.slow_range(50, 10), (0, 10))
        # 上界严格小于承诺线：(20,49] 全部 <50，不慢。
        fully_fast = Bucket(20, 49)
        self.assertEqual(fully_fast.slow_range(50, 4), (0, 0))
        # 桶 (20,50] 含延时恰为 50 的请求（慢口径为 >=50），仍属跨线桶。
        boundary_included = Bucket(20, 50)
        self.assertEqual(boundary_included.slow_range(50, 4), (0, 4))
        # 下界达到承诺线：(50,80] 全部慢。
        lower_on_line = Bucket(50, 80)
        self.assertEqual(lower_on_line.slow_range(50, 4), (4, 4))


class RateTests(unittest.TestCase):
    def test_integer_ppm_comparison(self):
        # 1/5 = 200000 ppm，不超 250000；2/5 = 400000 ppm，超。
        self.assertFalse(rate_breaches(1, 5, 250000))
        self.assertTrue(rate_breaches(2, 5, 250000))
        self.assertFalse(rate_breaches(0, 0, 250000))
        # 恰等于承诺线不算超标（严格大于）。
        self.assertFalse(rate_breaches(1, 4, 250000))  # 250000 ppm
        self.assertTrue(rate_breaches(1, 3, 250000))   # 333333 ppm


if __name__ == "__main__":
    unittest.main()
