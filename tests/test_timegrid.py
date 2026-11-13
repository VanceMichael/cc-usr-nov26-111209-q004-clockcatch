import unittest
from fractions import Fraction

from clockcatch.timegrid import (
    AnchorError,
    build_segments,
    classify_tick,
    parse_anchors,
    subtract_tick_intervals,
    window_anchor_gaps,
    window_tick_intervals,
)

# clock2 契约：A: 0->0us, 100->200_000_000us（每 tick 2,000,000us = 2s）
CLOCK2 = {
    'revision': 'clock2',
    'anchors': [
        {'bootId': 'A', 'tick': 0, 'utc_us': 0},
        {'bootId': 'A', 'tick': 100, 'utc_us': 200_000_000},
        {'bootId': 'B', 'tick': 0, 'utc_us': 120_000_000},
        {'bootId': 'B', 'tick': 80, 'utc_us': 280_000_000},
    ],
}
# clock3 修订：A: 0->0us, 100->150_000_000us（每 tick 1.5s，非整数秒比例）
CLOCK3 = {
    'revision': 'clock3',
    'anchors': [
        {'bootId': 'A', 'tick': 0, 'utc_us': 0},
        {'bootId': 'A', 'tick': 100, 'utc_us': 150_000_000},
        {'bootId': 'B', 'tick': 0, 'utc_us': 100_000_000},
        {'bootId': 'B', 'tick': 80, 'utc_us': 220_000_000},
    ],
}
SINGLE = {'revision': 'clock1',
          'anchors': [{'bootId': 'A', 'tick': 0, 'utc_us': 0}]}


class MappingTests(unittest.TestCase):
    def test_rational_linear_mapping_exact(self):
        segments = build_segments(parse_anchors(CLOCK3))['A']
        self.assertEqual(len(segments), 1)
        # 100 tick = 150s => 1 tick = 1.5s；tick 10 = 15s 精确值
        self.assertEqual(segments[0].map_to_time(10), Fraction(15_000_000))
        # 反查：15s -> tick 10
        self.assertEqual(segments[0].invert_time(15_000_000), Fraction(10))

    def test_window_inversion_clock2_60_160s(self):
        segments = build_segments(parse_anchors(CLOCK2))['A']
        # 2s/tick：60s->tick30，160s->tick80，端点都是整数
        self.assertEqual(window_tick_intervals(segments, 60_000_000, 160_000_000),
                         [(30, 80)])

    def test_window_inversion_clock3_moves_window(self):
        segments = build_segments(parse_anchors(CLOCK3))['A']
        # 1.5s/tick：60s->tick40，160s->tick106.67 截到区段边界 100
        self.assertEqual(window_tick_intervals(segments, 60_000_000, 160_000_000),
                         [(40, 100)])

    def test_fractional_boundaries_round_inward(self):
        segments = build_segments(parse_anchors(CLOCK3))['A']
        # 窗口 (15s, 150s) 对应 tick (10, 100)，开区间语义由向内取整实现
        intervals = window_tick_intervals(segments, 15_000_001, 149_999_999)
        self.assertEqual(intervals, [(11, 99)])

    def test_single_anchor_confirms_nothing(self):
        segments = build_segments(parse_anchors(SINGLE))['A']
        self.assertEqual(segments, [])
        self.assertEqual(window_tick_intervals(segments, 0, 10_000_000), [])
        gaps = window_anchor_gaps(segments, 60_000_000, 160_000_000)
        self.assertEqual(gaps, [(Fraction(60_000_000), Fraction(160_000_000))])

    def test_no_extrapolation_outside_segment(self):
        segments = build_segments(parse_anchors(CLOCK2))['A']
        status, business_time, segment = classify_tick(101, segments, 0, 10**12)
        self.assertEqual(status, 'tentative')
        self.assertIsNone(business_time)
        # 区段内但落在请求窗外：同样不得当作确认
        status, business_time, segment = classify_tick(90, segments,
                                                       60_000_000, 160_000_000)
        self.assertEqual(status, 'tentative')
        # 区段内且窗内：确认并给精确时间
        status, business_time, segment = classify_tick(40, segments,
                                                       60_000_000, 160_000_000)
        self.assertEqual(status, 'confirmed')
        self.assertEqual(business_time, Fraction(80_000_000))
        self.assertEqual((segment.anchor_from_ordinal, segment.anchor_until_ordinal),
                         (0, 1))

    def test_partial_window_anchor_gap(self):
        segments = build_segments(parse_anchors(CLOCK2))['A']
        # 区段时间范围 0..200s；请求 180s..240s，200s..240s 无锚点覆盖
        gaps = window_anchor_gaps(segments, 180_000_000, 240_000_000)
        self.assertEqual(gaps, [(Fraction(200_000_000), Fraction(240_000_000))])
        intervals = window_tick_intervals(segments, 180_000_000, 240_000_000)
        self.assertEqual(intervals, [(90, 100)])

    def test_interval_subtraction_and_merge(self):
        self.assertEqual(
            subtract_tick_intervals([(0, 100)], [(10, 39), (60, 70)]),
            [(0, 9), (40, 59), (71, 100)],
        )
        self.assertEqual(subtract_tick_intervals([(0, 10)], [(0, 10)]), [])

    def test_invalid_anchors_rejected(self):
        with self.assertRaises(AnchorError):
            parse_anchors({'revision': 'x', 'anchors': [{'bootId': 'A'}]})
        with self.assertRaises(AnchorError):
            build_segments(parse_anchors(
                {'revision': 'x', 'anchors': [
                    {'bootId': 'A', 'tick': 0, 'utc_us': 10},
                    {'bootId': 'A', 'tick': 10, 'utc_us': 10}]}))


if __name__ == '__main__':
    unittest.main()
