"""按业务时间区间组织的历史测量采集。

与 :mod:`clockcatch.archive` 的分工：archive 只保证原始响应不可变、可回放；
本模块负责把"业务时间窗 -> boot/tick 区间 -> 分页请求 -> 已确认/暂定测量"
整条链路做成可解释、可重算、可反查的采集结果。

设计要点：

* 同一 job 的多次 run 共享 append-only 的 sample_records，重采只补缺口；
* 旧 active run 的确认结果在新 run 满足覆盖条件前保持不动（原子切换）；
* 请求失败、重启（boot 清单变化）、同 tick 冲突值分别独立记账；
* 每条测量都能反查到原始 capture 与使用的两个校时锚点。
"""

import json
import sqlite3
import urllib.parse
from fractions import Fraction

from .archive import Archive
from . import timegrid
from .timegrid import (
    AnchorError,
    build_segments,
    classify_tick,
    parse_anchors,
    subtract_tick_intervals,
    window_anchor_gaps,
    window_tick_intervals,
)


class CollectionError(RuntimeError):
    """采集无法继续（前置接口失败等），run 已落库为 failed。"""


def _now(connection):
    return connection.execute('select current_timestamp').fetchone()[0]


class CollectionStore:
    def __init__(self, archive, database, fetcher=None):
        if not isinstance(archive, Archive):
            raise TypeError('需要 Archive 实例承载原始响应')
        self.archive = archive
        self.fetcher = fetcher
        self.connection = sqlite3.connect(database)
        self.connection.execute('pragma foreign_keys=on')
        self.connection.executescript(
            """
            create table if not exists jobs (
                job text primary key,
                start_us integer not null,
                end_us integer not null,
                scan_radius integer not null,
                created_at text not null
            );
            create table if not exists runs (
                id integer primary key,
                job text not null references jobs(job),
                clock_revision text,
                state text not null,
                note text,
                started_at text not null,
                finished_at text
            );
            create index if not exists run_job on runs(job);
            create table if not exists active_runs (
                job text primary key references jobs(job),
                run_id integer not null references runs(id)
            );
            create table if not exists anchor_snapshots (
                id integer primary key,
                run_id integer not null references runs(id),
                capture_id integer not null,
                clock_revision text not null,
                ordinal integer not null,
                boot_id text not null,
                tick integer not null,
                utc_us integer not null,
                unique(run_id, ordinal)
            );
            create table if not exists fetch_intervals (
                id integer primary key,
                run_id integer not null references runs(id),
                job text not null,
                boot_id text not null,
                tick_from integer not null,
                tick_until integer not null,
                raw_revision text not null
            );
            create index if not exists fetch_job_boot on fetch_intervals(job, boot_id);
            create table if not exists sample_records (
                id integer primary key,
                job text not null,
                boot_id text not null,
                tick integer not null,
                value text not null,
                raw_revision text not null,
                capture_id integer not null,
                run_id integer not null,
                unique(job, boot_id, tick, value, capture_id)
            );
            create index if not exists sample_identity on sample_records(job, boot_id, tick);
            create table if not exists measurements (
                id integer primary key,
                run_id integer not null references runs(id),
                job text not null,
                boot_id text not null,
                tick integer not null,
                status text not null,
                value text not null,
                business_us_num integer,
                business_us_den integer,
                record_id integer not null references sample_records(id),
                anchor_snapshot_id integer,
                anchor_from_ordinal integer,
                anchor_until_ordinal integer,
                unique(run_id, boot_id, tick)
            );
            create table if not exists conflicts (
                id integer primary key,
                run_id integer not null references runs(id),
                job text not null,
                boot_id text not null,
                tick integer not null,
                value text not null,
                record_id integer not null references sample_records(id),
                unique(run_id, boot_id, tick, value)
            );
            create table if not exists request_failures (
                id integer primary key,
                run_id integer not null references runs(id),
                job text not null,
                kind text not null,
                url text not null,
                http_status integer,
                detail text not null,
                capture_id integer,
                occurred_at text not null
            );
            create table if not exists events (
                id integer primary key,
                run_id integer not null references runs(id),
                job text not null,
                kind text not null,
                detail text not null,
                occurred_at text not null
            );
            create table if not exists coverage_entries (
                id integer primary key,
                run_id integer not null references runs(id),
                job text not null,
                boot_id text not null,
                kind text not null,
                status text not null,
                tick_from integer,
                tick_until integer,
                time_from_num integer,
                time_from_den integer,
                time_until_num integer,
                time_until_den integer,
                detail text
            );
            """
        )

    def close(self):
        self.connection.close()

    # ------------------------------------------------------------------ URLs

    @staticmethod
    def _url(base, path, **query):
        parsed = urllib.parse.urlsplit(base)
        encoded = urllib.parse.urlencode(query)
        return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, encoded, ''))

    def _http(self, run_id, job, url):
        """发请求并归档；任何失败都先记账，不抛出。

        成功返回 ``(status, payload, capture_id)``；传输层失败返回
        ``(None, None, None)``；HTTP 非 200 或 JSON 非法时 payload 为 None。
        """
        try:
            status, _content_type, body = self._call(url)
        except Exception as error:  # 网络层失败：没有 HTTP 响应可归档
            self._record_failure(run_id, job, 'transport', url, None, str(error), None)
            return None, None, None
        capture_id = self.archive.save(url, status, 'application/json', body)
        if status != 200:
            self._record_failure(run_id, job, 'http', url, status,
                                 f'HTTP {status}', capture_id)
            return status, None, capture_id
        try:
            payload = json.loads(body)
        except ValueError as error:
            self._record_failure(run_id, job, 'protocol', url, status,
                                 f'响应不是 JSON: {error}', capture_id)
            return status, None, capture_id
        return status, payload, capture_id

    def _call(self, url):
        if self.fetcher is None:
            from .transport import fetch
            return fetch(url)
        return self.fetcher(url)

    # --------------------------------------------------------------- 记账辅助

    def _record_failure(self, run_id, job, kind, url, http_status, detail, capture_id):
        with self.connection:
            self.connection.execute(
                'insert into request_failures(run_id,job,kind,url,http_status,detail,capture_id,occurred_at)'
                ' values(?,?,?,?,?,?,?,?)',
                (run_id, job, kind, url, http_status, detail, capture_id,
                 _now(self.connection)),
            )

    def _record_event(self, run_id, job, kind, detail):
        with self.connection:
            self.connection.execute(
                'insert into events(run_id,job,kind,detail,occurred_at) values(?,?,?,?,?)',
                (run_id, job, kind, detail, _now(self.connection)),
            )

    def _fail_run(self, run_id, note):
        with self.connection:
            self.connection.execute(
                'update runs set state=?, note=?, finished_at=? where id=?',
                ('failed', note, _now(self.connection), run_id),
            )

    # ------------------------------------------------------------------ 主流程

    def collect(self, base_url, job, start_us, end_us, scan_radius=30):
        """对一个业务时间窗执行一次采集 run，返回 run_id。

        单个 boot 的请求失败不会中断其他 boot；前置接口（/boots、/anchors）
        失败则无法规划，run 标记 failed 后抛出 :class:`CollectionError`。
        """
        if end_us < start_us:
            raise ValueError('业务时间窗起点晚于终点')
        if scan_radius < 0:
            raise ValueError('scan_radius 不能为负')
        with self.connection:
            existing = self.connection.execute(
                'select 1 from jobs where job=?', (job,)
            ).fetchone()
            if existing is None:
                self.connection.execute(
                    'insert into jobs(job,start_us,end_us,scan_radius,created_at)'
                    ' values(?,?,?,?,?)',
                    (job, start_us, end_us, scan_radius, _now(self.connection)),
                )
            run_id = self.connection.execute(
                'insert into runs(job,state,started_at) values(?,?,?)',
                (job, 'running', _now(self.connection)),
            ).lastrowid

        try:
            limit, boots, raw_limits = self._load_boots(run_id, job, base_url)
            clock_revision, segments_by_boot, anchor_capture = self._load_anchors(
                run_id, job, base_url
            )
        except CollectionError as error:
            self._fail_run(run_id, str(error))
            raise

        with self.connection:
            self.connection.execute(
                'update runs set clock_revision=? where id=?', (clock_revision, run_id)
            )

        had_failure = False
        for boot_id in boots:
            try:
                had_failure = self._collect_boot(
                    run_id, job, base_url, boot_id, limit,
                    segments_by_boot.get(boot_id, []),
                    start_us, end_us, scan_radius,
                ) or had_failure
            except CollectionError as error:
                # 规划级失败已经记账；继续其他 boot
                self._fail_run(run_id, str(error))
                had_failure = True
        for unknown in sorted(set(segments_by_boot) - set(boots)):
            self._record_event(
                run_id, job, 'anchor-boot-not-listed',
                f'锚点包含 /boots 未列出的启动期 {unknown}，无法抓取样本',
            )

        state = self._finalize(run_id, job, start_us, end_us, had_failure)
        self._promote(run_id, job, state)
        return run_id

    def _load_boots(self, run_id, job, base_url):
        url = self._url(base_url, '/boots')
        status, payload, capture_id = self._http(run_id, job, url)
        if payload is None:
            raise CollectionError(f'/boots 不可用（capture {capture_id}）')
        try:
            boots = list(payload['boots'])
            limit = int(payload['limit'])
            if not boots or limit <= 0 or not all(isinstance(b, str) for b in boots):
                raise ValueError('boots/limit 字段不合法')
        except (KeyError, TypeError, ValueError) as error:
            self._record_failure(run_id, job, 'protocol', url, status,
                                 f'/boots 契约不符: {error}', capture_id)
            raise CollectionError('/boots 契约不符') from error
        previous = self.connection.execute(
            'select boot_id from (select distinct boot_id from fetch_intervals where job=?)',
            (job,),
        ).fetchall()
        previous_boots = {row[0] for row in previous}
        if previous_boots:
            added = sorted(set(boots) - previous_boots)
            removed = sorted(previous_boots - set(boots))
            if added:
                self._record_event(run_id, job, 'restart',
                                   f'新启动期出现（源序号会重置）: {",".join(added)}')
            if removed:
                self._record_event(run_id, job, 'restart',
                                   f'启动期从清单消失: {",".join(removed)}')
        return limit, boots, capture_id

    def _load_anchors(self, run_id, job, base_url):
        url = self._url(base_url, '/anchors')
        status, payload, capture_id = self._http(run_id, job, url)
        if payload is None:
            raise CollectionError(f'/anchors 不可用（capture {capture_id}）')
        try:
            grouped = parse_anchors(payload)
            revision = payload['revision']
            if not isinstance(revision, str):
                raise AnchorError('锚点 revision 缺失')
            segments_by_boot = build_segments(grouped)
        except AnchorError as error:
            self._record_failure(run_id, job, 'protocol', url, status,
                                 f'锚点契约不符: {error}', capture_id)
            raise CollectionError('锚点契约不符') from error
        with self.connection:
            for boot_anchors in grouped.values():
                for anchor in boot_anchors:
                    self.connection.execute(
                        'insert into anchor_snapshots'
                        '(run_id,capture_id,clock_revision,ordinal,boot_id,tick,utc_us)'
                        ' values(?,?,?,?,?,?,?)',
                        (run_id, capture_id, revision, anchor.ordinal,
                         anchor.boot_id, anchor.tick, anchor.utc_us),
                    )
        return revision, segments_by_boot, capture_id

    # --------------------------------------------------------------- 单 boot

    def _collect_boot(self, run_id, job, base_url, boot_id, limit, segments,
                      start_us, end_us, scan_radius):
        had_failure = False
        target_intervals = window_tick_intervals(segments, start_us, end_us)
        planned = list(target_intervals)
        if not segments:
            # 没有任何有界区段：无法确认时间，仅允许按固定半径暂定扫描，不外推
            planned.append((0, scan_radius))
            self._coverage(
                run_id, job, boot_id, 'scan', 'tentative', 0, scan_radius,
                detail='无成对锚点，业务时间无法确认；按扫描半径暂定收集',
            )
        planned = timegrid.merge_tick_intervals(planned)

        # 窗口内缺锚点（有锚点但区段盖不住）的时间缺口必须明示
        for gap_start, gap_end in window_anchor_gaps(segments, start_us, end_us):
            self._coverage(
                run_id, job, boot_id, 'anchor_gap', 'missing',
                time_from=gap_start, time_until=gap_end,
                detail='业务时间窗内无有界锚点区段，该部分只能暂定/缺失，禁止外推',
            )
        # 每个 run 独立完整抓取本次规划区间：历史 run 的相同记录会幂等去重，
        # 但重采必须重新观察源端，否则新出现的冲突值将永远无法被发现。
        gaps = subtract_tick_intervals(planned,
                                       self._covered_intervals(job, boot_id, run_id))

        for interval_from, interval_until in gaps:
            if not self._page_interval(run_id, job, base_url, boot_id,
                                       interval_from, interval_until, limit):
                had_failure = True

        # 覆盖度解释：目标 tick 区段逐段判定本次 run 实际取到了哪里
        actual = self._covered_intervals(job, boot_id, run_id)
        for target_from, target_until in target_intervals:
            missing = subtract_tick_intervals([(target_from, target_until)], actual)
            status = 'missing' if missing else 'covered'
            self._coverage(
                run_id, job, boot_id, 'target', status,
                target_from, target_until,
                detail=('仍缺 tick 区段: ' + self._format_intervals(missing))
                if missing else '目标 tick 区段已全部取到',
            )
        return had_failure

    def _covered_intervals(self, job, boot_id, run_id=None):
        if run_id is None:
            rows = self.connection.execute(
                'select tick_from,tick_until from fetch_intervals where job=? and boot_id=?',
                (job, boot_id),
            ).fetchall()
        else:
            rows = self.connection.execute(
                'select tick_from,tick_until from fetch_intervals'
                ' where job=? and boot_id=? and run_id=?',
                (job, boot_id, run_id),
            ).fetchall()
        return timegrid.merge_tick_intervals([(a, b) for a, b in rows])

    def _page_interval(self, run_id, job, base_url, boot_id,
                       interval_from, interval_until, limit):
        """在一个 tick 区间内按源记录上限持续拆页，直到取完或失败。

        源端上限是"记录条数"而非 tick 跨度（样本可能稀疏、同 tick 还可能有
        冲突值），因此每次请求 ``[cursor, interval_until]`` 全剩余区间，按
        本页最后一条记录的 tick 推进：

        * truncated=false：源端担保整个剩余区间再无记录，按全区间记账；
        * truncated=true：只把 ``[cursor, last_tick]`` 记为已覆盖，
          并以 last_tick（含）续查以兜住同 tick 的冲突值；
        * 截断页却没有带来任何新记录 => 源端翻页停滞，记账并终止。
        """
        cursor = interval_from
        while cursor <= interval_until:
            url = self._url(base_url, '/samples', boot=boot_id,
                            **{'from': cursor, 'until': interval_until})
            status, payload, capture_id = self._http(run_id, job, url)
            if payload is None:
                return False
            try:
                samples = self._validate_samples(payload, boot_id)
                truncated = bool(payload.get('truncated'))
                raw_revision = payload.get('revision')
                if not isinstance(raw_revision, str):
                    raise ValueError('raw revision 缺失')
            except (TypeError, ValueError) as error:
                self._record_failure(run_id, job, 'protocol', url, status,
                                     f'/samples 契约不符: {error}', capture_id)
                return False

            new_records = 0
            for sample in samples:
                record_id = self._store_sample(
                    run_id, job, boot_id, sample['tick'],
                    str(sample['value']), raw_revision, capture_id,
                )
                if record_id is not None:
                    new_records += 1
            last_tick = samples[-1]['tick'] if samples else None

            if not truncated:
                # 源端明示剩余区间无更多记录，整段担保为已覆盖
                covered_until = interval_until
            else:
                if new_records == 0:
                    self._record_failure(
                        run_id, job, 'protocol', url, status,
                        'truncated 响应没有带来任何新记录，判定为源端翻页停滞，终止该区间',
                        capture_id,
                    )
                    return False
                covered_until = last_tick

            with self.connection:
                self.connection.execute(
                    'insert into fetch_intervals'
                    '(run_id,job,boot_id,tick_from,tick_until,raw_revision)'
                    ' values(?,?,?,?,?,?)',
                    (run_id, job, boot_id, cursor, covered_until, raw_revision),
                )

            if not truncated:
                break
            cursor = last_tick  # 含端点续查，确保同 tick 冲突值也能取到
        return True

    @staticmethod
    def _validate_samples(payload, boot_id):
        if not isinstance(payload, dict) or payload.get('bootId') != boot_id:
            raise ValueError('响应 bootId 与请求不一致')
        samples = payload.get('samples')
        if not isinstance(samples, list):
            raise ValueError('samples 不是数组')
        last_tick = None
        for sample in samples:
            tick = sample.get('tick')
            if not isinstance(tick, int) or isinstance(tick, bool) or tick < 0:
                raise ValueError('tick 必须是非负整数')
            if not isinstance(sample.get('value'), str):
                raise ValueError('value 必须是十进制文本')
            if last_tick is not None and tick < last_tick:
                raise ValueError('样本未按 tick 排序')
            last_tick = tick
        return samples

    def _store_sample(self, run_id, job, boot_id, tick, value, raw_revision, capture_id):
        """append-only 落库；已存在的完全相同记录返回 None（幂等重采）。"""
        try:
            with self.connection:
                cursor = self.connection.execute(
                    'insert into sample_records'
                    '(job,boot_id,tick,value,raw_revision,capture_id,run_id)'
                    ' values(?,?,?,?,?,?,?)',
                    (job, boot_id, tick, value, raw_revision, capture_id, run_id),
                )
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None

    def _coverage(self, run_id, job, boot_id, kind, status,
                  tick_from=None, tick_until=None, time_from=None, time_until=None,
                  detail=None):
        def parts(value):
            if value is None:
                return None, None
            fraction = value if isinstance(value, Fraction) else Fraction(value)
            return fraction.numerator, fraction.denominator
        f_num, f_den = parts(time_from)
        u_num, u_den = parts(time_until)
        with self.connection:
            self.connection.execute(
                'insert into coverage_entries'
                '(run_id,job,boot_id,kind,status,tick_from,tick_until,'
                'time_from_num,time_from_den,time_until_num,time_until_den,detail)'
                ' values(?,?,?,?,?,?,?,?,?,?,?,?)',
                (run_id, job, boot_id, kind, status, tick_from, tick_until,
                 f_num, f_den, u_num, u_den, detail),
            )

    # --------------------------------------------------------------- 收尾切换

    def _finalize(self, run_id, job, start_us, end_us, had_failure):
        # 测量只由"本次 run 实际观察到"的记录构成：历史 run 的记录用于补缺口，
        # 但不参与本次状态判定，否则冲突会被旧值稀释、失败会被旧数据掩盖。
        rows = self.connection.execute(
            'select distinct boot_id, tick from sample_records where run_id=?',
            (run_id,),
        ).fetchall()
        anchors = self.connection.execute(
            'select ordinal,boot_id,tick,utc_us from anchor_snapshots where run_id=?',
            (run_id,),
        ).fetchall()
        grouped = {}
        for ordinal, boot_id, tick, utc_us in anchors:
            grouped.setdefault(boot_id, []).append(
                timegrid.Anchor(boot_id, tick, utc_us, ordinal)
            )
        try:
            segments_by_boot = build_segments(grouped)
        except AnchorError as error:
            self._fail_run(run_id, f'锚点无法构成映射: {error}')
            return 'failed'

        conflict_pairs = self.connection.execute(
            'select boot_id,tick from sample_records where run_id=?'
            ' group by boot_id,tick having count(distinct value)>1',
            (run_id,),
        ).fetchall()
        conflict_set = {(boot_id, tick) for boot_id, tick in conflict_pairs}

        confirmed_count = 0
        with self.connection:
            for boot_id, tick in sorted(rows):
                segments = segments_by_boot.get(boot_id, [])
                status, business_time, segment = classify_tick(
                    tick, segments, start_us, end_us
                )
                value_row = self.connection.execute(
                    'select id,value from sample_records where run_id=? and boot_id=? and tick=?'
                    ' order by id limit 1',
                    (run_id, boot_id, tick),
                ).fetchone()
                record_id, value = value_row
                if (boot_id, tick) in conflict_set:
                    status = 'conflicted'
                if status == 'confirmed':
                    confirmed_count += 1
                self.connection.execute(
                    'insert into measurements'
                    '(run_id,job,boot_id,tick,status,value,business_us_num,'
                    'business_us_den,record_id,anchor_snapshot_id,'
                    'anchor_from_ordinal,anchor_until_ordinal)'
                    ' values(?,?,?,?,?,?,?,?,?,?,?,?)',
                    (run_id, job, boot_id, tick, status, value,
                     business_time.numerator if business_time else None,
                     business_time.denominator if business_time else None,
                     record_id, None,
                     segment.anchor_from_ordinal if segment else None,
                     segment.anchor_until_ordinal if segment else None),
                )
            for boot_id, tick in sorted(conflict_set):
                # 每个冲突值只记一条；同值的重复记录（分页重叠）天然去重
                for value, in self.connection.execute(
                    'select distinct value from sample_records'
                    ' where run_id=? and boot_id=? and tick=? order by value',
                    (run_id, boot_id, tick),
                ):
                    record_id = self.connection.execute(
                        'select id from sample_records'
                        ' where run_id=? and boot_id=? and tick=? and value=?'
                        ' order by id limit 1',
                        (run_id, boot_id, tick, value),
                    ).fetchone()[0]
                    self.connection.execute(
                        'insert into conflicts(run_id,job,boot_id,tick,value,record_id)'
                        ' values(?,?,?,?,?,?)',
                        (run_id, job, boot_id, tick, value, record_id),
                    )

        if had_failure:
            state = 'failed'
        elif conflict_set:
            state = 'conflicted'
        elif confirmed_count:
            state = 'confirmed'
        else:
            state = 'tentative'
        with self.connection:
            self.connection.execute(
                'update runs set state=?, finished_at=? where id=?',
                (state, _now(self.connection), run_id),
            )
        return state

    def _promote(self, run_id, job, state):
        """决定是否把对外 active 结果切换到新 run（旧确认结果先保留）。"""
        with self.connection:
            active = self.connection.execute(
                'select run_id from active_runs where job=?', (job,)
            ).fetchone()
            if active is None:
                if state in {'tentative', 'confirmed'}:
                    # 首个 run：暂定也先挂出，但报告明确它不是确认结果；
                    # 冲突/失败状态不具备挂出资格
                    self.connection.execute(
                        'insert into active_runs(job,run_id) values(?,?)', (job, run_id)
                    )
                    self._record_event(run_id, job, 'promoted',
                                       f'首个 run 挂为当前结果（状态 {state}）')
                else:
                    self._record_event(run_id, job, 'promotion-withheld',
                                       f'首个 run 状态为 {state}，不挂出当前结果')
                return
            if state == 'confirmed':
                # 新映射与应取区段都满足覆盖条件后才原子切换
                self.connection.execute(
                    'update active_runs set run_id=? where job=?', (run_id, job)
                )
                self._record_event(run_id, job, 'promoted',
                                   f'覆盖条件满足，确认结果切换到 run {run_id}')
            else:
                self._record_event(
                    run_id, job, 'promotion-withheld',
                    f'新 run 状态为 {state}，保留 run {active[0]} 的旧结果',
                )

    # ------------------------------------------------------------------ 查询

    @staticmethod
    def _fraction(num, den):
        return Fraction(num, den) if num is not None and den is not None else None

    def report(self, run_id):
        run = self.connection.execute(
            'select id,job,clock_revision,state,note,started_at,finished_at from runs where id=?',
            (run_id,),
        ).fetchone()
        if run is None:
            raise KeyError(run_id)
        job, clock_revision, state = run[1], run[2], run[3]
        active = self.connection.execute(
            'select run_id from active_runs where job=?', (job,)
        ).fetchone()
        job_row = self.connection.execute(
            'select start_us,end_us,scan_radius from jobs where job=?', (job,)
        ).fetchone()
        measurements = [
            self._measurement_row(row)
            for row in self.connection.execute(
                'select run_id,boot_id,tick,status,value,business_us_num,business_us_den,'
                'record_id,anchor_from_ordinal,anchor_until_ordinal'
                ' from measurements where run_id=? order by boot_id,tick', (run_id,)
            )
        ]
        coverage = [self._coverage_row(row) for row in self.connection.execute(
            'select boot_id,kind,status,tick_from,tick_until,'
            'time_from_num,time_from_den,time_until_num,time_until_den,detail'
            ' from coverage_entries where run_id=? order by boot_id,kind,tick_from',
            (run_id,),
        )]
        return {
            'run_id': run[0],
            'job': job,
            'window_us': [job_row[0], job_row[1]],
            'scan_radius': job_row[2],
            'clock_revision': clock_revision,
            'state': state,
            'note': run[4],
            'is_active': bool(active and active[0] == run_id),
            'active_run_id': active[0] if active else None,
            'boots': sorted({m['boot_id'] for m in measurements}),
            'counts': {
                'confirmed': sum(1 for m in measurements if m['status'] == 'confirmed'),
                'tentative': sum(1 for m in measurements if m['status'] == 'tentative'),
                'conflicted': sum(1 for m in measurements if m['status'] == 'conflicted'),
                'failures': self.connection.execute(
                    'select count(*) from request_failures where run_id=?', (run_id,)
                ).fetchone()[0],
                'conflicts': self.connection.execute(
                    'select count(*) from conflicts where run_id=?', (run_id,)
                ).fetchone()[0],
            },
            'measurements': measurements,
            'coverage': coverage,
            'failures': [self._failure_row(row) for row in self.connection.execute(
                'select kind,url,http_status,detail,capture_id,occurred_at'
                ' from request_failures where run_id=? order by id', (run_id,)
            )],
            'events': [dict(zip(['kind', 'detail'], row)) for row in self.connection.execute(
                'select kind,detail from events where run_id=? order by id', (run_id,)
            )],
        }

    def _measurement_row(self, row):
        business = self._fraction(row[5], row[6])
        return {
            'boot_id': row[1], 'tick': row[2], 'status': row[3], 'value': row[4],
            'business_utc_us': int(business) if business is not None and business.denominator == 1
            else (float(business) if business is not None else None),
            'business_utc_us_exact': str(business) if business is not None else None,
            'record_id': row[7],
            'anchor_ordinals': [row[8], row[9]] if row[8] is not None else None,
        }

    def _coverage_row(self, row):
        time_from = self._fraction(row[5], row[6])
        time_until = self._fraction(row[7], row[8])
        return {
            'boot_id': row[0], 'kind': row[1], 'status': row[2],
            'tick_interval': [row[3], row[4]] if row[3] is not None else None,
            'time_interval_us': [
                float(time_from) if time_from is not None else None,
                float(time_until) if time_until is not None else None,
            ],
            'time_interval_exact': [
                str(time_from) if time_from is not None else None,
                str(time_until) if time_until is not None else None,
            ],
            'detail': row[9],
        }

    @staticmethod
    def _failure_row(row):
        return {'kind': row[0], 'url': row[1], 'http_status': row[2],
                'detail': row[3], 'capture_id': row[4], 'occurred_at': row[5]}

    def trace(self, job, boot_id, tick):
        """从任意测量反查原始响应与使用的校时锚点。"""
        records = self.connection.execute(
            'select id,value,raw_revision,capture_id,run_id from sample_records'
            ' where job=? and boot_id=? and tick=? order by id',
            (job, boot_id, tick),
        ).fetchall()
        if not records:
            raise KeyError((job, boot_id, tick))
        result = {'job': job, 'boot_id': boot_id, 'tick': tick, 'observations': []}
        for record_id, value, raw_revision, capture_id, run_id in records:
            capture = self.archive.get_capture(capture_id)
            entry = {
                'record_id': record_id,
                'value': value,
                'raw_revision': raw_revision,
                'first_seen_run': run_id,
                'raw_response': {
                    'capture_id': capture_id,
                    'url': capture['url'],
                    'http_status': capture['status'],
                    'content_type': capture['content_type'],
                    'sha256': capture['digest'],
                    'body': capture['body'].decode('utf-8'),
                },
            }
            result['observations'].append(entry)
        # 当前 active run 如何解释这条测量
        active = self.connection.execute(
            'select run_id from active_runs where job=?', (job,)
        ).fetchone()
        if active:
            measurement = self.connection.execute(
                'select status,value,business_us_num,business_us_den,'
                'anchor_from_ordinal,anchor_until_ordinal,record_id,run_id'
                ' from measurements where run_id=? and boot_id=? and tick=?',
                (active[0], boot_id, tick),
            ).fetchone()
            if measurement:
                anchors = []
                if measurement[4] is not None:
                    for ordinal in (measurement[4], measurement[5]):
                        anchor = self.connection.execute(
                            'select boot_id,tick,utc_us,capture_id,clock_revision'
                            ' from anchor_snapshots where run_id=? and ordinal=?',
                            (measurement[7], ordinal),
                        ).fetchone()
                        anchor_capture = self.archive.get_capture(anchor[3])
                        anchors.append({
                            'ordinal': ordinal,
                            'boot_id': anchor[0], 'tick': anchor[1], 'utc_us': anchor[2],
                            'clock_revision': anchor[4],
                            'anchor_response': {
                                'capture_id': anchor[3],
                                'url': anchor_capture['url'],
                                'http_status': anchor_capture['status'],
                                'sha256': anchor_capture['digest'],
                            },
                        })
                business = self._fraction(measurement[2], measurement[3])
                result['active_interpretation'] = {
                    'run_id': measurement[7],
                    'status': measurement[0],
                    'value': measurement[1],
                    'business_utc_us_exact': str(business) if business is not None else None,
                    'anchors_used': anchors,
                }
        return result

    def runs(self, job):
        return [dict(zip(['run_id', 'clock_revision', 'state', 'note',
                          'started_at', 'finished_at'], row))
                for row in self.connection.execute(
                    'select id,clock_revision,state,note,started_at,finished_at'
                    ' from runs where job=? order by id', (job,))]

    @staticmethod
    def _format_intervals(intervals):
        return ';'.join(f'{a}-{b}' for a, b in intervals) or '无'
