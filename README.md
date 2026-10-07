# data-quality-gate-python

本地数据质量校验与监控服务：用本地 JSON 数据集模拟上游输入，对数据集并发执行
多类型校验规则，结论留痕、可复现、可解释，并支持规则版本化、部分失败重跑，
以及**跟着数据内容变化做持续监控**：内容版本化留痕、跨内容版本的结论差异对比、
整轮墙钟时限保护。

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
- `error`：规则**执行失败**（异常或超时），原因中携带异常类型/超时阈值。

## 终态定义

一次运行的整体终态（`RunStatus`）：

| 终态 | 条件 |
| --- | --- |
| `passed` | 所有规则在所有分片上均 `passed` |
| `failed` | 至少一条 `failed`，且无任何 `error`/`rejected` |
| `partial` | 存在 `error`（含超时）或 `rejected`，结论不完整——**部分失败绝不会让整体静默变为通过** |

## 版本与重跑语义

结论留痕存储（`dqgate.store.ResultStore`，JSONL 追加写）以
`(dataset_id, dataset_digest, rule_id, rule_version, shard)` 为键：

- **可复现**：数据集指纹（行内容的 SHA-256）与规则版本共同决定结论；
  同一数据集在同一规则版本下重跑，结论逐条一致，重复记录相同结论为幂等 no-op。
- **运行标识可复现（run_id）**：`run_id` 由「数据内容指纹 + 规则集
  （id/类型/版本/参数）+ 分片数」做 SHA-256 确定性派生（见
  `runner.derive_run_id`）。**同一份数据内容配同一套规则版本，无论隔多久跑、
  还是服务重启之后再跑，run_id 与每条结论完全一致**，不随运行时刻漂移；数据
  内容一变，指纹与 run_id 随之改变。
- **内容版本化留痕**：数据集内容变化会产生新的 `dataset_digest`。新旧内容版本
  的结论以各自的指纹独立留痕、**互不覆盖、互不报错**；同一个 `dataset_id` 下
  可并存任意多个内容版本，可按数据集 + 内容版本分别查出，也可通过
  `list_digests(dataset_id)` 列出该数据集全部内容版本。
- **不篡改**：键已存在且既有记录为终态结论（`passed`/`failed`）时，
  写入不一致结论会被拒绝（`DuplicateResultError`）；规则升级（版本 +1）后
  重新校验生成新键，旧版本结论原样保留，可按版本区分查询。
- **非终态可收敛**：`error`/`rejected` 表示"未得出结论"，故障恢复后重跑允许
  追加新结论取代索引；JSONL 文件只追加，历史失败痕迹可通过
  `query_history(...)`（或接口 `GET /results?history=true`）全量追溯。
- **只重跑失败部分**：`rerun_failed_only=True` 时，**同一内容版本下**已有
  `passed` 留痕的 (规则, 版本, 分片) 直接复用不重复执行；`failed`/`error`/
  `rejected`/无留痕的部分重新执行（`failed` 为确定性结论，重跑结果一致，幂等写入）。

## 持续监控：跨内容版本的批次差异对比

`dqgate.monitor` 在「同一数据集、同一套规则版本」下，对比前后两个数据内容版本
的结论（按 `(rule_id, rule_version, shard)` 对齐）：

| 分类 | 判定 | 含义 |
| --- | --- | --- |
| `new_violations` | 旧 `passed` → 新 `failed` | 本次**新出现**的违规 |
| `fixed_violations` | 旧 `failed` → 新 `passed` | 原来的违规**已修好** |
| `unresolved_violations` | 旧 `failed` → 新 `failed` | **还没解决**的违规 |
| `inconclusive` | 任一侧为 `error`/`rejected`/缺失等 | **未得出结论**，绝不悄悄算作修好或新违规 |

- `BatchDiff.counts` 给出四类条数；`counts_by_rule()` / `counts_by_shard()`
  可下钻到具体是哪条规则、哪个分片；每条差异带有前后状态。
- 内存/报告对比：`diff_results(dataset_id, old_digest, new_digest, old, new)`；
  从留痕直接对比：`diff_stored(store, dataset_id, old_digest, new_digest)`；
  HTTP：`GET /diff?dataset_id=...&old_digest=...&new_digest=...`。
- 两侧同为 `passed` 的键无差异，不计入任何告警类。

## 并发、分片与整轮时限

`QualityRunner` 用守护线程池并发执行「规则 × 分片」任务，分片按行号取模稳定
切分（可复现）。两种超时保护：

- **单任务超时** `rule_timeout_sec`：某条规则（在某个分片上）执行超过阈值，记为
  `error`，不影响其他任务。
- **整轮墙钟时限** `run_deadline_sec`（新增）：覆盖排队、执行与收尾的整轮耗时。
  到点 `run()` 必须返回——仍在排队或卡死的规则统一记为 `error`
  （原因含"整轮校验超过墙钟时限"，即**没得出结论，绝不当通过**），同一轮里其他
  规则已经跑出来的 `passed`/`failed` 结论照常保留并留痕。卡死线程为 daemon，
  不会阻止进程退出。不传该参数时行为与旧版一致（仅受单任务超时约束）。

> 注意：`duplicate_record` 在分片内检测重复；若需全局去重，请使用
> `num_shards=1`。

## 本地验证方法

```bash
# 运行全部单测（原有用例：并发、版本切换、部分失败/超时、可复现；
# 新增用例：内容版本化留痕与跨重启可复现、批次差异判定、卡死规则的整轮时限）
# -s + --log-cli-level=INFO：日志可看出覆盖了哪些业务与每个用例的逐条结论
uv run pytest tests/ -q -s --log-cli-level=INFO

# 只跑本次新增的持续监控用例
uv run pytest tests/test_monitoring.py -v -s --log-cli-level=INFO

# 启动服务
uv run python main.py   # 或 uv run uvicorn main:app

# 发起校验（示例数据集 data/sample_dataset.json）
curl -X POST localhost:8000/validate -H 'content-type: application/json' -d '{
  "dataset_path": "data/sample_dataset.json",
  "num_shards": 2,
  "run_deadline_sec": 5,
  "rules": [
    {"rule_id": "amount-present", "rule_type": "missing_field", "version": 1, "params": {"field": "amount"}},
    {"rule_id": "amount-range", "rule_type": "value_range", "version": 1, "params": {"field": "amount", "min": 0}},
    {"rule_id": "ts-order", "rule_type": "cross_field", "version": 1, "params": {"left": "created_ts", "right": "updated_ts", "op": "le"}}
  ]
}'

# 按版本查询历史结论（可加 dataset_digest 只查某个内容版本；
# 不加则返回该数据集所有内容版本的结论；加 history=true 可看含被取代记录的全量留痕）
curl 'localhost:8000/results?dataset_id=orders-demo&rule_id=amount-range&rule_version=1'

# 列出某数据集全部内容版本
curl 'localhost:8000/datasets/orders-demo/digests'

# 对比前后两个内容版本：新出现/已修好/未解决/未得出结论 的条数与下钻
curl 'localhost:8000/diff?dataset_id=orders-demo&old_digest=<旧指纹>&new_digest=<新指纹>'
```

结果存储文件默认为 `./dq_results.jsonl`，可用环境变量 `DQ_STORE_PATH` 覆盖。

### 兼容范围（历史数据与老结论）

- **旧留痕文件可直接读**：旧版本写出的每条 JSONL 记录本就带有
  `dataset_digest` 字段，本次只是把它加入存储键，老文件无需迁移即可加载查询。
- **旧 API 调用不受影响**：`GET /results` 的 `(dataset_id, rule_id,
  rule_version)` 位置参数语义保持不变；新过滤参数 `dataset_digest`、
  `history` 均为可选；`/validate` 的 `run_deadline_sec` 可选（不传时限时，
  执行行为与旧版一致）。
- **内容变化不再被当成冲突**：旧实现下"同数据集、同规则版本但内容变了"会被
  防篡改逻辑拒绝；现在因内容指纹入键，新版本结论与旧版本共存，各自可查。
- `ResultStore.get(...)` 新签名需要传入 `dataset_digest`（仅执行器内部使用）；
  `query/query_history` 的 `dataset_digest` 仅以关键字传参，以兼容旧的位置调用。

## 代码结构

```
dqgate/
  models.py   # 类型定义：规则、结论、终态
  dataset.py  # 本地数据集加载、内容指纹、稳定分片
  rules.py    # 规则定义校验（拒绝非法）与五类规则执行
  store.py    # JSONL 追加式留痕：按 (数据集,内容版本,规则版本,分片) 幂等写、
              #   防覆盖、按内容/规则版本查询、全量留痕、内容版本列表
  runner.py   # 确定性 run_id、守护线程池并发、单任务超时与整轮墙钟时限、
              #   终态聚合、失败重跑
  monitor.py  # 跨内容版本差异对比：新出现/已修好/未解决/未得出结论，按规则分片下钻
main.py       # FastAPI 入口：/validate、/results、/datasets/{id}/digests、/diff
tests/
  test_dqgate.py      # 并发、版本切换、部分失败、可复现性（原有，不回归）
  test_monitoring.py  # 内容版本化留痕与可复现、批次差异判定、卡死规则时限保证（新增）
```
