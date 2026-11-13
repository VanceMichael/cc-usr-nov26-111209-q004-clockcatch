"""可重复的端到端采集演练。

剧本对应 docs/source-contract.md 推荐的业务窗口 [60s, 160s]：

1. initial 相位只有单锚点，任何 tick 都无法确认业务时间 —— 先得到暂定结果，
   并明示缺锚点、扫描半径、分页拆取过程；
2. complete 相位（clock2）成对锚点齐备，重算窗口 tick 区间并扩大抓取，
   覆盖满足后原子切换为确认结果；
3. revised 相位（clock3）映射修订，窗口对应 tick 区间改变：新进入窗口的
   tick 被补抓，被移出的 tick 不再出现在确认测量中，旧 run 结果可追溯；
4. conflict 相位制造 A:20 的冲突值，演示同 tick 冲突独立记账、切换被阻止；
5. unavailable 相位样本接口 503，演示请求失败独立记账且不污染已有结果；
6. 最后从一条已确认测量反查原始响应与所用校时锚点。
"""

import json
import shutil
import tempfile
import threading
from pathlib import Path

from .archive import Archive
from .collector import CollectionStore
from .source_server import FixtureServer
from .transport import fetch

WINDOW = (60_000_000, 160_000_000)
SCAN_RADIUS = 100


def _phase(base, value):
    status, _content_type, body = fetch(base + '/control/phase?value=' + value)
    assert status == 200, body


def run_demo(workdir=None):
    server = FixtureServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = 'http://127.0.0.1:' + str(server.server_address[1])
    created = workdir is None
    if created:
        workdir = tempfile.mkdtemp(prefix='clockcatch-demo-')
    workdir = Path(workdir)
    archive = Archive(workdir / 'captures.sqlite3', workdir / 'objects')
    store = CollectionStore(archive, workdir / 'collections.sqlite3')
    transcript = []
    try:
        def step(title, phase, job, expect_state=None):
            if phase is not None:
                _phase(base, phase)
            window = (0, WINDOW[1]) if job == 'wide-demo' else WINDOW
            run_id = store.collect(base, job, window[0], window[1],
                                   scan_radius=SCAN_RADIUS)
            report = store.report(run_id)
            if expect_state is not None:
                assert report['state'] == expect_state, report['state']
            summary = {
                'title': title,
                'phase': phase or server.phase,
                'run_id': run_id,
                'clock_revision': report['clock_revision'],
                'state': report['state'],
                'is_active': report['is_active'],
                'counts': report['counts'],
                'boots': report['boots'],
                'coverage': report['coverage'],
                'events': report['events'],
                'confirmed_ticks': [
                    [m['boot_id'], m['tick'], m['business_utc_us_exact']]
                    for m in report['measurements'] if m['status'] == 'confirmed'
                ],
                'tentative_ticks': [
                    [m['boot_id'], m['tick']]
                    for m in report['measurements'] if m['status'] == 'tentative'
                ],
            }
            transcript.append(summary)
            return report

        # 1. 只有单锚点：暂定结果 + 缺锚点解释
        first = step('单锚点：暂定采集，不确认业务时间', 'initial', 'window-demo',
                     expect_state='tentative')

        # 2. 校时补齐：映射有界，重算并扩大抓取，原子切换为确认
        second = step('成对锚点（clock2）：扩大抓取并确认', 'complete', 'window-demo',
                      expect_state='confirmed')

        # 3. 映射修订（clock3）：tick 窗口改变，移出的 tick 不再确认
        third = step('映射修订（clock3）：重算窗口，补新排旧', 'revised', 'window-demo',
                     expect_state='confirmed')

        # 4. 冲突相位：同 tick 两个值，冲突独立记账，切换被阻止。
        #    冲突夹具造在 A:20，需要用覆盖该 tick 的更宽窗口作业才能观察到。
        conflict = step('冲突值：同 tick 冲突，阻止切换', 'conflict', 'wide-demo',
                        expect_state='conflicted')
        # 宽窗作业此前没有 active run，首个冲突 run 会被挂为当前结果但明确不是确认

        # 5. 不可用相位：503 失败独立记账，旧确认结果保持 active
        failed = step('接口 503：失败记账，保留旧确认结果', 'unavailable', 'window-demo',
                      expect_state='failed')

        # 6. 从一条已确认测量反查原始响应与校时锚点。
        #    clock3 下 tick 60 的业务时间是 60*1.5s=90s（修订后与 clock2 的 120s 不同）
        trace = store.trace('window-demo', 'A', 60)
        assert trace['active_interpretation']['business_utc_us_exact'] == '90000000'

        result = {
            'window_us': list(WINDOW),
            'workdir': str(workdir),
            'runs': store.runs('window-demo'),
            'wide_window_runs': store.runs('wide-demo'),
            'steps': transcript,
            'conflict_detail': [
                {'boot_id': row[0], 'tick': row[1], 'values': row[2]}
                for row in store.connection.execute(
                    'select boot_id,tick,group_concat(distinct value) from conflicts'
                    ' where run_id=? group by boot_id,tick',
                    (conflict['run_id'],))
            ],
            'failure_detail': failed['failures'],
            'trace_A_60': trace,
            'retained_active_run': failed['active_run_id'],
        }
        return result
    finally:
        store.close()
        archive.close()
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
        if created:
            shutil.rmtree(workdir, ignore_errors=True)


def main():
    print(json.dumps(run_demo(), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
