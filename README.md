# data-quality-gate-python

本地数据质量校验与监控服务：用本地 JSON 数据集模拟上游输入，对数据集并发执行
多类型校验规则，结论留痕、可复现、可解释，支持规则版本化、部分失败重跑，并提供
**跟随数据内容变化的持续监控**：按数据内容版本留痕、跨内容版本对比违规差异、
整轮校验墙钟时限保证。

## 规则模型

规则定义（`dqgate.models.RuleDefinition`）：

| 字段 | 说明 |
| --- | --- |
| `rule_id` | 规则标识，非空 |
| `rule_type` | 规则类型，见下表 |
| `version` | 规则版本，>=1 的整数；同一 `rule_id` 的不同版本相互独立 |
| `params` | 规则参数，按类型校验合法性 |

支持的规则类型与参数：

| rule_type | 含义 | params |
| --- | --- | --- |
| `missing_field` | 字段缺失/为空 | `field` |
| `value_range` | 取值越界/非数值 | `field`, `min`?, `max`?（至少其一） |
| `duplicate_record` | 重复记录 | `keys`: 非空字段列表 |
| `freshness` | 数据新鲜度 | `field`, `max_age_seconds`, `now`?（固定参考时间以保证可复现） |
| `cross_field` | 跨字段一致性 | `left`, `right`, `op` ∈ `eq/ne/lt/le/gt/ge` |

单条规则的执行结论（`CheckStatus`）：

- `passed` / `failed`：真正执行完成后的终态结论，附 `reason`（判定依据）与
  `evidence`（违例计数与样本）。
- `rejected`：规则**定义非法**（缺参数、类型错误、版本非法等），拒绝执行，
  原因可区分，绝不静默跳过或默认通过。
- `error`：规则**未得出结论**（执行异常、单条超时或整轮时限到点），
  原因中携带异常类型/超时阈值，绝不当作通过。

## 终态定义

一次运行的整体终态（`RunStatus`）：

| 终态 | 条件 |
| --- | --- |
| `passed` | 所有规则在所有分片上均 `passed` |
| `failed` | 至少一条 `failed`，且无任何 `error`/`rejected` |
| `partial` | 存在 `error`（含超时/整轮到点）或 `rejected`，结论不完整——**部分失败绝不会让整体静默变为通过** |

## 持续监控：内容版本化留痕、可复现、批次差异

### 确定性运行标识（可复现）

`run_id` 默认由 **数据集标识 + 数据内容指纹（行内容 SHA-256）+ 规则集合指纹
（规则 id/类型/版本/参数）+ 分片数** 确定性派生（`dqgate.identity`），
与运行时刻、进程、主机无关。同一份数据内容配同一套规则版本：

- 隔多久再跑、服务重启后再跑，`run_id` 完全一致，逐条结论与判定依据一致；
- 只有数据内容或规则集合真正变化时，`run_id` 才变化。
- 仍可在 API 显式传入 `run_id`（与旧用法兼容）。

### 按数据内容版本分别留痕、互不覆盖

留痕键从 v1 的 `(dataset_id, rule_id, rule_version, shard)` 扩展为
`(dataset_id, dataset_digest, rule_id, rule_version, shard)`：

- 同一个数据集、同一套规则版本下，**数据内容一变就是一个新的内容版本**，
  新老内容版本的结论各自留痕、互不覆盖，也不会报“重复结论”错误；
- 可按 `dataset_id` 单独查，也可按 `dataset_id + dataset_digest` 精确查某个
  内容版本的结论；`GET /datasets/{id}/content-versions` 列出该数据集历史上
  跑过的全部内容版本及各版本结论计数。
- 同一内容版本重跑为幂等 no-op，结论不重复、不漂移；不一致的伪造结论仍被
  `DuplicateResultError` 拒绝。

### 批次差异：新出现 / 已修好 / 未解决

`dqgate.monitor.diff_content(...)` /
`POST /datasets/{id}/diff` 对比同一数据集、同一规则版本下前后两个数据内容版本。
违规以 `(rule_id, rule_version, shard)` 跨版本对齐：

| 类别 | 含义 |
| --- | --- |
| `new` | 新出现的违规（旧版本无、新版本有） |
| `fixed` | 已修好的违规（旧版本有、新版本无） |
| `unresolved` | 仍未解决（两个版本都有） |

返回三类各自的**条数**，以及可下钻的明细（规则 id、规则版本、分片、判定依据、
证据样本）；只对终态数据结论 `failed` 计违规，`error`/`rejected`（未得出结论）
不会被悄悄当作“已修好”。可传 `rule_version` 只看某一规则版本。

## 整轮墙钟时限（卡死规则保护）

`QualityRunner(run_deadline_sec=...)` / 请求参数 `run_deadline_sec` 设定一次校验
（**含并发执行与结论收尾**）的墙钟硬上限：

- 到点必须返回；卡死、特别慢或仍在排队的规则一律记为 `error`，原因为
  “整轮校验时限到点…规则未得出结论（不计为通过）”，**绝不静默当通过**；
- 同一轮里其他规则已经跑出来的结论不受影响，整体终态为 `partial`；
- `rule_timeout_sec` 仍是逐任务独立起算的软超时，与整轮时限取更紧者生效；
- 到点不等待卡死线程收尾（`shutdown(wait=False, cancel_futures=True)`），
  因此调用方一定在时限内拿到返回。

## 版本与重跑语义

结论留痕存储（`dqgate.store.ResultStore`，JSONL 追加写）：

- **不篡改**：键已存在且结论本体（status/reason/evidence）不一致、既有记录为
  终态（`passed`/`failed`）时，写入被拒绝；规则升级（版本 +1）后生成新键，
  旧版本结论原样保留，可按版本区分查询。
- **非终态可收敛**：`error`/`rejected` 表示“未得出结论”，故障恢复后重跑允许
  追加新结论取代索引；JSONL 文件只追加，历史失败痕迹留痕可查。
- **只重跑失败部分**：`rerun_failed_only=True` 时，**同一数据内容版本下**已有
  `passed` 留痕的 (规则, 版本, 分片) 直接复用不重复执行；数据内容变了
  （digest 不同）绝不复用别的内容版本的结论；`failed`/`error`/`rejected`/
  无留痕的部分重新执行（`failed` 为确定性结论，幂等写入）。

## 兼容范围（历史数据与老结论）

- **老 JSONL 文件直接可用**：v1 写出的记录没有 `run_id` 字段，加载时归一化为
  `run_id="legacy"`，原有 `dataset_digest` 字段 v1 已具备，内容版本化对历史
  留痕直接生效，无需迁移脚本。
- **幂等比对忽略 `run_id` 元数据**：只比较结论本体（status/reason/evidence），
  历史老结论在新运行下补跑不会被误判为冲突，也不会重复落盘。
- **API 向后兼容**：`POST /validate` 不传 `run_deadline_sec` 时行为与旧版一致
  （仅保留单条 `rule_timeout_sec`）；`GET /results` 新增的
  `dataset_digest`/`run_id` 均为可选过滤参数。
- 老结论按新键解释后，同样可参与 `diff` 批次对比。

## 并发与分片

`QualityRunner` 用线程池并发执行「规则 × 分片」任务，单任务超时记为
`error` 且不影响其他任务；分片按行号取模稳定切分（可复现），结论按任务提交
顺序聚合，报告内容确定。

> 注意：`duplicate_record` 在分片内检测重复；若需全局去重，请使用
> `num_shards=1`。

## 本地验证方法

```bash
# 运行全部单测（日志逐条打印覆盖的业务场景：输入摘要/规则版本/判定依据、
# 内容版本留痕、批次差异三类计数、整轮时限墙钟耗时）
uv run pytest tests/ -q -s --log-cli-level=INFO

# 只看本次新增的监控能力用例
uv run pytest tests/test_monitoring.py -v --log-cli-level=INFO

# 启动服务
uv run python main.py   # 或 uv run uvicorn main:app

# 1) 校验同一份数据两次（run_id 应一致）；改数据后再校验（新内容版本）
curl -X POST localhost:8000/validate -H 'content-type: application/json' -d '{
  "dataset_path": "data/sample_dataset.json",
  "run_deadline_sec": 5,
  "rules": [
    {"rule_id": "amount-range", "rule_type": "value_range", "version": 1, "params": {"field": "amount", "min": 0}}
  ]
}'

# 2) 列出数据集的历史内容版本
curl localhost:8000/datasets/orders-demo/content-versions

# 3) 对比两个内容版本的违规差异（digest 来自前两步响应）
curl -X POST localhost:8000/datasets/orders-demo/diff -H 'content-type: application/json' -d '{
  "old_digest": "<旧内容指纹>", "new_digest": "<新内容指纹>"
}'

# 4) 按数据内容版本 / 运行标识查询历史结论
curl 'localhost:8000/results?dataset_id=orders-demo&dataset_digest=<内容指纹>'
curl 'localhost:8000/runs?dataset_id=orders-demo'
```

结果存储文件默认为 `./dq_results.jsonl`，可用环境变量 `DQ_STORE_PATH` 覆盖。

## 代码结构

```
dqgate/
  models.py   # 类型定义：规则、结论、终态、违规差异（ContentDiff/ViolationItem/DiffKind）
  dataset.py  # 本地数据集加载、内容指纹、稳定分片
  identity.py # 确定性指纹：规则集合指纹与可复现 run_id 派生
  rules.py    # 规则定义校验（拒绝非法）与五类规则执行
  store.py    # JSONL 追加式留痕：按数据内容版本存、幂等写、防覆盖、多维查询
  runner.py   # 线程池并发、单条软超时、整轮墙钟硬时限、终态聚合、失败重跑
  monitor.py  # 内容版本列表与批次差异判定（新出现/已修好/未解决）
main.py       # FastAPI 入口：/validate、/results、/content-versions、/diff、/runs
tests/
  test_dqgate.py     # 原有能力：规则类型、并发、部分失败、版本切换、可复现
  test_monitoring.py # 新增：内容版本化留痕与可复现、批次差异、整轮时限、老文件兼容
  conftest.py        # 公共夹具
```
