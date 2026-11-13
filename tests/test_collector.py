import json
import tempfile
import threading
import unittest
from pathlib import Path

from clockcatch.archive import Archive
from clockcatch.collector import CollectionError, CollectionStore
from clockcatch.source_server import FixtureServer
from clockcatch.transport import fetch

WINDOW = (60_000_000, 160_000_000)


class CollectorFixture(unittest.TestCase):
    def setUp(self):
        self.server = FixtureServer()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = 'http://127.0.0.1:' + str(self.server.server_address[1])
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.archive = Archive(root / 'captures.sqlite3', root / 'objects')
        self.store = CollectionStore(self.archive, root / 'collections.sqlite3')

    def tearDown(self):
        self.store.close()
        self.archive.close()
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()
        self.directory.cleanup()

    def phase(self, value):
        status, _type, body = fetch(self.base + '/control/phase?value=' + value)
        self.assertEqual(status, 200, body)

    def collect(self, job='job', **kwargs):
        return self.store.collect(self.base, job, WINDOW[0], WINDOW[1], **kwargs)


class TentativeThenConfirmedTests(CollectorFixture):
    def test_initial_is_tentative_with_explained_gaps(self):
        run_id = self.collect(scan_radius=100)
        report = self.store.report(run_id)
        self.assertEqual(report['state'], 'tentative')
        self.assertEqual(report['clock_revision'], 'clock1')
        self.assertEqual(report['counts']['confirmed'], 0)
        self.assertTrue(report['measurements'])
        # 每个 boot 都有缺锚点解释，且没有任何外推时间
        gap_boots = {c['boot_id'] for c in report['coverage'] if c['kind'] == 'anchor_gap'}
        self.assertEqual(gap_boots, {'A', 'B'})
        self.assertTrue(all(m['business_utc_us_exact'] is None
                            for m in report['measurements']))

    def test_complete_phase_confirms_and_paginates(self):
        first = self.collect(scan_radius=100)
        self.phase('complete')
        second = self.collect()
        report = self.store.report(second)
        self.assertEqual(report['state'], 'confirmed')
        self.assertEqual(report['clock_revision'], 'clock2')
        # clock2：A 2s/tick => 窗口 tick 30..80，样本每 10 tick：30,40,50,60,70,80
        confirmed_a = [(m['tick'], m['business_utc_us_exact'])
                       for m in report['measurements']
                       if m['boot_id'] == 'A' and m['status'] == 'confirmed']
        self.assertEqual([tick for tick, _ in confirmed_a], [30, 40, 50, 60, 70, 80])
        self.assertEqual(dict(confirmed_a)[60], '120000000')
        # B：时间 120s..280s 映射 0..80 tick；窗口 60s..160s 只与 120s..160s 相交
        confirmed_b = [m['tick'] for m in report['measurements']
                       if m['boot_id'] == 'B' and m['status'] == 'confirmed']
        self.assertEqual(confirmed_b, [0, 20])
        # limit=3 强制分页：归档的 /samples 请求数明显大于 boot 数
        sample_captures = [r for r in self.archive.rows() if '/samples' in r['url']]
        self.assertGreater(len(sample_captures), 4)
        # 旧暂定 run 保留可查，active 已切换
        self.assertEqual(self.store.report(first)['state'], 'tentative')
        self.assertTrue(report['is_active'])
        self.assertFalse(self.store.report(first)['is_active'])

    def test_revision_moves_window_and_idempotent_records(self):
        self.phase('complete')
        run_clock2 = self.collect()
        records_before = self.store.connection.execute(
            'select count(*) from sample_records').fetchone()[0]
        self.phase('revised')
        run_clock3 = self.collect()
        report3 = self.store.report(run_clock3)
        self.assertEqual(report3['state'], 'confirmed')
        # clock3：A 1.5s/tick => 60s->tick40, 160s->tick106.7 截到 100
        confirmed_a = [m['tick'] for m in report3['measurements']
                       if m['boot_id'] == 'A' and m['status'] == 'confirmed']
        self.assertEqual(confirmed_a, [40, 50, 60, 70, 80, 90, 100])
        # 重采重新观察窗口，但完全相同的记录幂等去重：新增量仅来自新进入窗口的 tick
        records_after = self.store.connection.execute(
            'select count(*) from sample_records').fetchone()[0]
        new_ticks = {row[0] for row in self.store.connection.execute(
            'select distinct tick from sample_records where run_id=? and boot_id="A"',
            (run_clock3,))}
        self.assertTrue(new_ticks.issuperset({90, 100}))
        self.assertGreater(records_after, records_before)
        # clock2 确认过的 tick 30 被修订移出窗口：clock3 run 的测量集不再包含它，
        # 但 append-only 的历史记录与旧 run 仍可追溯
        self.assertNotIn(30, new_ticks)
        # 旧确认 run 仍然完整可追溯
        report2 = self.store.report(run_clock2)
        self.assertEqual(report2['state'], 'confirmed')
        self.assertTrue(report3['is_active'])


class ConflictAndFailureTests(CollectorFixture):
    WIDE_WINDOW = (0, 160_000_000)  # 覆盖到冲突夹具所在的 A:20

    def collect_wide(self, job='wide'):
        return self.store.collect(self.base, job, self.WIDE_WINDOW[0],
                                  self.WIDE_WINDOW[1], scan_radius=100)

    def test_conflicting_tick_recorded_and_blocks_promotion(self):
        self.phase('complete')
        good_run = self.collect_wide()
        self.phase('conflict')
        run_id = self.collect_wide()
        report = self.store.report(run_id)
        self.assertEqual(report['state'], 'conflicted')
        self.assertEqual(report['counts']['conflicts'], 2)  # A:20 的两个值
        conflicted = [m for m in report['measurements'] if m['status'] == 'conflicted']
        self.assertEqual([(m['boot_id'], m['tick']) for m in conflicted], [('A', 20)])
        # 冲突不允许切换，旧确认 run 继续 active
        self.assertFalse(report['is_active'])
        self.assertTrue(self.store.report(good_run)['is_active'])

    def test_unavailable_records_failures_and_keeps_old_result(self):
        self.phase('complete')
        good_run = self.collect()
        self.phase('unavailable')
        run_id = self.collect()
        report = self.store.report(run_id)
        self.assertEqual(report['state'], 'failed')
        self.assertTrue(report['failures'])
        self.assertTrue(all(f['kind'] == 'http' and f['http_status'] == 503
                            for f in report['failures']))
        # 503 原文同样归档，可回放
        capture = self.archive.read(report['failures'][0]['capture_id'])
        self.assertEqual(json.loads(capture)['error'], 'offline')
        self.assertEqual(report['active_run_id'], good_run)
        self.assertTrue(self.store.report(good_run)['is_active'])

    def test_transport_failure_is_recorded(self):
        def broken_fetcher(url):
            raise OSError('connection reset')
        self.store.fetcher = broken_fetcher
        with self.assertRaises(CollectionError):
            self.collect()
        run_id = self.store.runs('job')[-1]['run_id']
        report = self.store.report(run_id)
        self.assertEqual(report['state'], 'failed')
        self.assertEqual(report['failures'][0]['kind'], 'transport')

    def test_unknown_boot_404_recorded_per_boot_without_poisoning_others(self):
        self.phase('complete')
        real_fetch = fetch

        def with_unknown_boot(url):
            if url.endswith('/boots'):
                status, ctype, body = real_fetch(url)
                payload = json.loads(body)
                payload['boots'] = ['A', 'GHOST']
                return status, ctype, json.dumps(payload).encode()
            return real_fetch(url)

        self.store.fetcher = with_unknown_boot
        run_id = self.collect()
        report = self.store.report(run_id)
        # A 正常确认，GHOST 的 404 单独记账，整体 run 标记失败且不得晋升
        self.assertEqual(report['state'], 'failed')
        self.assertTrue(any(m['boot_id'] == 'A' and m['status'] == 'confirmed'
                            for m in report['measurements']))
        self.assertTrue(any(f['http_status'] == 404 for f in report['failures']))
        self.assertFalse(report['is_active'])

    def test_window_outside_all_segments_is_explained_not_extrapolated(self):
        self.phase('complete')
        run_id = self.store.collect(self.base, 'far',
                                    900_000_000, 999_000_000, scan_radius=0)
        report = self.store.report(run_id)
        self.assertEqual(report['state'], 'tentative')
        self.assertEqual(report['counts']['confirmed'], 0)
        self.assertTrue(all(c['kind'] == 'anchor_gap' and c['status'] == 'missing'
                            for c in report['coverage']))


class TraceTests(CollectorFixture):
    def test_trace_returns_raw_response_and_anchors(self):
        self.phase('complete')
        run_id = self.collect()
        trace = self.store.trace('job', 'A', 60)
        observation = trace['observations'][0]
        self.assertEqual(observation['raw_revision'], 'raw1')
        # 原始响应可直接回放，且就是归档对象本身
        body = json.loads(observation['raw_response']['body'])
        self.assertEqual(body['bootId'], 'A')
        self.assertIn({'tick': 60, 'value': '26'}, body['samples'])
        interpretation = trace['active_interpretation']
        self.assertEqual(interpretation['status'], 'confirmed')
        self.assertEqual(interpretation['business_utc_us_exact'], '120000000')
        anchors = interpretation['anchors_used']
        self.assertEqual([(a['tick'], a['utc_us']) for a in anchors],
                         [(0, 0), (100, 200_000_000)])
        # 锚点响应本身也可反查到原始 capture
        anchor_capture = self.archive.read(anchors[0]['anchor_response']['capture_id'])
        self.assertEqual(json.loads(anchor_capture)['revision'], 'clock2')


class RestartTests(CollectorFixture):
    def test_new_boot_is_logged_as_restart(self):
        self.phase('complete')
        self.collect(job='fleet')
        # 模拟重启后清单变化：用一个只报 boot A 的假 fetcher 包装真实 fetch
        real_fetch = fetch

        def filtered(url):
            if url.endswith('/boots'):
                status, ctype, body = real_fetch(url)
                payload = json.loads(body)
                payload['boots'] = ['A']
                return status, ctype, json.dumps(payload).encode()
            return real_fetch(url)

        self.store.fetcher = filtered
        self.collect(job='fleet')
        run_id = self.store.runs('fleet')[-1]['run_id']
        events = self.store.report(run_id)['events']
        self.assertTrue(any(e['kind'] == 'restart' and 'B' in e['detail']
                            for e in events), events)


if __name__ == '__main__':
    unittest.main()
