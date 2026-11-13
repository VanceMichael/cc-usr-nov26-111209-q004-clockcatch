DESCRIPTION = '温控记录器区间接口的原始样本查看、业务时间窗采集与可解释归档。'

import argparse
import json
from pathlib import Path

from .archive import Archive
from .collector import CollectionError, CollectionStore
from .demo import run_demo
from .parser import inspect_document
from .transport import fetch


def _open_store(arguments):
    archive = Archive(arguments.db, arguments.objects)
    store = CollectionStore(archive, arguments.collections)
    return archive, store


def main():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    commands = parser.add_subparsers(dest='command', required=True)
    inspect = commands.add_parser('inspect', help='查看一份源文件的结构')
    inspect.add_argument('path')
    capture = commands.add_parser('capture', help='发起一次本机 HTTP 请求并保存原文')
    capture.add_argument('url')
    capture.add_argument('--db', default='captures.sqlite3')
    capture.add_argument('--objects', default='objects')
    show = commands.add_parser('show', help='列出原始接收记录')
    show.add_argument('--db', default='captures.sqlite3')
    show.add_argument('--objects', default='objects')

    collect = commands.add_parser('collect', help='按业务时间区间采集一个作业的历史测量')
    collect.add_argument('base_url', help='记录器接口根地址，仅允许本机 HTTP')
    collect.add_argument('job')
    collect.add_argument('--start-us', type=int, required=True,
                         help='业务时间窗起点，相对 2026-11-12T00:00:00Z 的微秒')
    collect.add_argument('--end-us', type=int, required=True, help='业务时间窗终点（含）')
    collect.add_argument('--scan-radius', type=int, default=30,
                         help='无成对锚点时每 boot 的暂定扫描 tick 半径，默认 30')
    collect.add_argument('--db', default='captures.sqlite3')
    collect.add_argument('--objects', default='objects')
    collect.add_argument('--collections', default='collections.sqlite3')

    report = commands.add_parser('report', help='查看一次采集 run 的可解释报告')
    report.add_argument('run_id', type=int)
    report.add_argument('--db', default='captures.sqlite3')
    report.add_argument('--objects', default='objects')
    report.add_argument('--collections', default='collections.sqlite3')

    runs = commands.add_parser('runs', help='列出一个作业的全部采集 run')
    runs.add_argument('job')
    runs.add_argument('--db', default='captures.sqlite3')
    runs.add_argument('--objects', default='objects')
    runs.add_argument('--collections', default='collections.sqlite3')

    trace = commands.add_parser('trace', help='从测量反查原始响应与使用的校时锚点')
    trace.add_argument('job')
    trace.add_argument('boot_id')
    trace.add_argument('tick', type=int)
    trace.add_argument('--db', default='captures.sqlite3')
    trace.add_argument('--objects', default='objects')
    trace.add_argument('--collections', default='collections.sqlite3')

    commands.add_parser('demo', help='运行暂定->确认->修订->冲突->失败->反查演练')
    arguments = parser.parse_args()

    if arguments.command == 'inspect':
        result = inspect_document(Path(arguments.path).read_bytes())
    elif arguments.command in {'capture', 'show'}:
        archive = Archive(arguments.db, arguments.objects)
        try:
            if arguments.command == 'capture':
                status, content_type, body = fetch(arguments.url)
                result = {'capture_id': archive.save(arguments.url, status, content_type, body),
                          'http_status': status}
            else:
                result = archive.rows()
        finally:
            archive.close()
    elif arguments.command == 'demo':
        result = run_demo()
    else:
        archive, store = _open_store(arguments)
        try:
            if arguments.command == 'collect':
                try:
                    run_id = store.collect(
                        arguments.base_url, arguments.job,
                        arguments.start_us, arguments.end_us,
                        scan_radius=arguments.scan_radius,
                    )
                except CollectionError as error:
                    raise SystemExit(f'采集无法继续: {error}')
                result = store.report(run_id)
            elif arguments.command == 'report':
                result = store.report(arguments.run_id)
            elif arguments.command == 'runs':
                result = {'job': arguments.job, 'runs': store.runs(arguments.job)}
            else:
                try:
                    result = store.trace(arguments.job, arguments.boot_id, arguments.tick)
                except KeyError:
                    raise SystemExit('未找到该测量')
        finally:
            store.close()
            archive.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
