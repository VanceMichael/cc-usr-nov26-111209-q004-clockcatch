"""校时锚点与业务时间映射。

契约（见 docs/source-contract.md）：

* 锚点按 bootId 分组，同一启动期内相邻两个锚点构成一个有界区段；
* 区段闭区间内使用精确有理数线性映射，区间之外一律不得确认业务时间；
* 跨 bootId 的 tick 不可比较，不存在跨启动期映射。

本模块只做纯计算，不发起任何请求，便于单独测试。所有时间均为相对于
2026-11-12T00:00:00Z 的整数微秒；映射结果可能不是整数微秒，因此以
``Fraction`` 精确传递，仅在落库展示时才取舍。
"""

from dataclasses import dataclass
from fractions import Fraction

EPOCH = '2026-11-12T00:00:00Z'


class AnchorError(ValueError):
    """锚点数据不满足契约（tick 非递增、时间倒挂等）。"""


@dataclass(frozen=True)
class Anchor:
    boot_id: str
    tick: int
    utc_us: int
    ordinal: int


@dataclass(frozen=True)
class Segment:
    """同一 boot 内一对相邻锚点围成的有界闭区段。"""

    boot_id: str
    tick_from: int
    tick_until: int
    utc_from: int
    utc_until: int
    anchor_from_ordinal: int
    anchor_until_ordinal: int

    def map_to_time(self, tick):
        """tick -> 业务时间（Fraction 微秒），调用方须保证 tick 在区段内。"""
        slope = Fraction(self.utc_until - self.utc_from, self.tick_until - self.tick_from)
        return Fraction(self.utc_from) + slope * (tick - self.tick_from)

    def invert_time(self, utc_us):
        """业务时间 -> tick（Fraction），调用方须保证时间在区段时间范围内。"""
        slope = Fraction(self.tick_until - self.tick_from, self.utc_until - self.utc_from)
        return Fraction(self.tick_from) + slope * (Fraction(utc_us) - self.utc_from)

    def contains_tick(self, tick):
        return self.tick_from <= tick <= self.tick_until


def parse_anchors(payload):
    """解析 /anchors 响应，返回 ``{boot_id: [Anchor, ...]}``（tick 升序）。"""
    if not isinstance(payload, dict) or not isinstance(payload.get('anchors'), list):
        raise AnchorError('锚点响应缺少 anchors 数组')
    grouped = {}
    for ordinal, raw in enumerate(payload['anchors']):
        if not isinstance(raw, dict):
            raise AnchorError('锚点必须是对象')
        boot_id = raw.get('bootId')
        tick = raw.get('tick')
        utc_us = raw.get('utc_us')
        if not isinstance(boot_id, str) or not boot_id:
            raise AnchorError('锚点缺少 bootId')
        if not isinstance(tick, int) or isinstance(tick, bool) or tick < 0:
            raise AnchorError('锚点 tick 必须是非负整数')
        if not isinstance(utc_us, int) or isinstance(utc_us, bool):
            raise AnchorError('锚点 utc_us 必须是整数')
        grouped.setdefault(boot_id, []).append(Anchor(boot_id, tick, utc_us, ordinal))
    for boot_id, anchors in grouped.items():
        anchors.sort(key=lambda anchor: anchor.tick)
        previous_tick = None
        for anchor in anchors:
            if previous_tick is not None and anchor.tick == previous_tick:
                raise AnchorError(f'boot {boot_id} 存在重复 tick 锚点')
            previous_tick = anchor.tick
    return grouped


def build_segments(anchors_by_boot):
    """把每个 boot 的相邻锚点两两组成有界区段。"""
    segments_by_boot = {}
    for boot_id, anchors in anchors_by_boot.items():
        segments = []
        for left, right in zip(anchors, anchors[1:]):
            if right.utc_us <= left.utc_us:
                raise AnchorError(
                    f'boot {boot_id} 锚点 {left.tick}->{right.tick} 业务时间非递增，无法建立映射'
                )
            segments.append(
                Segment(
                    boot_id,
                    left.tick,
                    right.tick,
                    left.utc_us,
                    right.utc_us,
                    left.ordinal,
                    right.ordinal,
                )
            )
        segments_by_boot[boot_id] = segments
    return segments_by_boot


def _ceil(fraction):
    return -((-fraction.numerator) // fraction.denominator)


def _floor(fraction):
    return fraction.numerator // fraction.denominator


def merge_tick_intervals(intervals):
    """合并整数闭区间，相邻（b+1 == c）也算连续。"""
    ordered = sorted((int(a), int(b)) for a, b in intervals if a <= b)
    merged = []
    for start, end in ordered:
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _merge_fraction_intervals(intervals):
    ordered = sorted(intervals)
    merged = []
    for start, end in ordered:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def window_tick_intervals(segments, start_us, end_us):
    """业务时间窗反查 tick 整数闭区间（已合并）。

    只统计有界区段覆盖到的部分；没有区段就返回空列表，调用方据此判暂定。
    """
    if end_us < start_us:
        raise AnchorError('业务时间窗起点晚于终点')
    intervals = []
    window_start, window_end = Fraction(start_us), Fraction(end_us)
    for segment in segments:
        seg_start, seg_end = Fraction(segment.utc_from), Fraction(segment.utc_until)
        overlap_start = max(window_start, seg_start)
        overlap_end = min(window_end, seg_end)
        if overlap_start > overlap_end:
            continue
        tick_start = max(segment.tick_from, _ceil(segment.invert_time(overlap_start)))
        tick_end = min(segment.tick_until, _floor(segment.invert_time(overlap_end)))
        if tick_start <= tick_end:
            intervals.append((tick_start, tick_end))
    return merge_tick_intervals(intervals)


def window_anchor_gaps(segments, start_us, end_us):
    """业务时间窗内没有任何有界区段覆盖的时间子区间（Fraction 微秒）。"""
    covered = _merge_fraction_intervals(
        [
            (max(Fraction(start_us), Fraction(segment.utc_from)),
             min(Fraction(end_us), Fraction(segment.utc_until)))
            for segment in segments
            if segment.utc_from <= end_us and segment.utc_until >= start_us
        ]
    )
    gaps = []
    cursor = Fraction(start_us)
    for covered_start, covered_end in covered:
        if covered_start > cursor:
            gaps.append((cursor, covered_start))
        cursor = max(cursor, covered_end)
    if cursor < Fraction(end_us):
        gaps.append((cursor, Fraction(end_us)))
    return gaps


def classify_tick(tick, segments, start_us, end_us):
    """判定一个 tick 能否确认业务时间。

    返回 ``(status, business_time, segment)``：status 为 ``confirmed`` 时
    business_time 是 Fraction 且严格落在请求窗内；否则为 ``tentative``、
    business_time 为 None —— 区外绝不外推时间。
    """
    for segment in segments:
        if not segment.contains_tick(tick):
            continue
        business_time = segment.map_to_time(tick)
        if Fraction(start_us) <= business_time <= Fraction(end_us):
            return 'confirmed', business_time, segment
        return 'tentative', None, segment
    return 'tentative', None, None


def subtract_tick_intervals(needed, covered):
    """从 needed 整数闭区间中扣除 covered，返回仍缺的区间列表。"""
    covered = merge_tick_intervals(covered)
    remaining = []
    for need_start, need_end in merge_tick_intervals(needed):
        cursor = need_start
        for cover_start, cover_end in covered:
            if cover_start > need_end:
                break
            if cover_end < cursor:
                continue
            if cover_start > cursor:
                remaining.append((cursor, cover_start - 1))
            cursor = max(cursor, cover_end + 1)
            if cursor > need_end:
                break
        if cursor <= need_end:
            remaining.append((cursor, need_end))
    return merge_tick_intervals(remaining)
