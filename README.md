# clockcatch

温控记录器区间接口的原始样本查看、按业务时间区间的历史采集与可解释归档。
只交付采集事实（已确认/暂定测量、覆盖度、失败、冲突、反查链路），不做任何
温度异常预测。

## 能力

### 原始层（既有）

inspect 查看一份源文件的结构；capture 发起一次本机 HTTP 请求并保存响应状态、
原始字节和摘要；show 列出接收记录。原文对象按 SHA-256 共用，独立请求分别记账。
错误 HTTP 状态也会保存，原文归档不等于业务接收完成。

### 业务时间窗采集（新增）

* `timegrid.py`：相邻锚点的精确有理数（`Fraction`）线性映射与时间窗反查。
  只有成对锚点围成的有界闭区段能确认业务时间，区外只标暂定，零外推。
* `collector.py`：业务时间窗 → 各 boot 的 tick 区间 → 按源记录上限持续拆页
  → 已确认/暂定测量。迟到锚点修订后重算窗口：新进入的 tick 补抓，被移出的
  tick 不再进入确认集。
* 重采原子切换：新 run 满足覆盖条件前，上一版 active 结果保持不动；首个
  run 若是暂定状态会挂出但明确标注未确认，冲突/失败状态不挂出。
* 三类事实分别记账：请求失败（传输层 / 非 200 / 契约不符）、重启（boot 清单
  变化，源序号重置）、同 tick 冲突值（append-only 保留全部观测）。
* 每次查询都带覆盖度解释：覆盖了哪些 boot、哪些目标 tick 区段已取齐、哪些
  仍缺数据、业务窗内哪些时间段缺锚点。
* `trace` 从任意测量反查原始响应（含 SHA-256 与正文）及所用的两个校时锚点。

## 运行

使用 Python 3.11 或更新版本，仅依赖标准库，无安装步骤。全部示例均为本地合成
资料，不包含真实个人信息、商业凭据或线上地址。

```sh
# 单文档观察与原文归档（既有）
python3 -m clockcatch inspect clockcatch/fixtures/samples.json
python3 -m clockcatch capture http://127.0.0.1:8765/sample

# 端到端演练：暂定 -> 校时补齐扩大抓取并确认 -> 映射修订 -> 冲突 -> 503 -> 反查
python3 -m clockcatch demo

python3 -m unittest discover -s tests -v
python3 -m compileall -q clockcatch
```

手动采集需要先起源服务 `python3 -m clockcatch.source_server --port 8765`，
另一个终端执行（时间单位为相对 2026-11-12T00:00:00Z 的微秒）：

```sh
python3 -m clockcatch collect http://127.0.0.1:8765 车次-A \
  --start-us 60000000 --end-us 160000000
python3 -m clockcatch report 1          # run 报告：测量、覆盖度、失败、事件
python3 -m clockcatch runs 车次-A        # 该作业全部 run 与状态
python3 -m clockcatch trace 车次-A A 60  # 反查测量的原始响应与校时锚点
```

数据库默认落在当前目录：`captures.sqlite3`（原始请求）、`objects/`（SHA-256
原文对象）、`collections.sqlite3`（采集事实），均可通过参数指定独立位置。

## 采集状态语义

| 状态 | 含义 |
| --- | --- |
| `confirmed` | 测量 tick 落在成对锚点的有界区段内，且映射时间在请求业务窗内 |
| `tentative` | 缺成对锚点或 tick 在有界区段外，无法确认业务时间，禁止外推 |
| `conflicted` | 同一 bootId+tick 观察到不同 value，冲突值全部保留并阻止结果切换 |
| `failed` | 本次 run 存在请求失败；覆盖度报告会标注仍缺的 tick 区段 |

## 模块

`parser.py` 单文档观察；`archive.py` 不可变原文与请求事实；`transport.py`
限制实验请求到本机；`source_server.py` 来源示例；`timegrid.py` 锚点映射纯
计算；`collector.py` 采集编排、分页、重算切换与反查；`demo.py` 剧本演练。
来源边界及字段含义见 `docs/source-contract.md`。这些入口不会声称一次响应
已经构成完整目录或业务批次。

## 容器

Dockerfile 使用 Python 3.11 标准库镜像，不需要应用依赖下载。可用
`docker build -t clockcatch:local .` 构建，再执行
`docker run --rm --network none clockcatch:local`。本地 HTTP 与 PTY 演练
不需要外部网络，测试及单次演示通常数秒完成。
