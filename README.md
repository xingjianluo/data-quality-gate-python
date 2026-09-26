# data-quality-gate-python

本地数据质量校验与监控服务：用本地 JSON 数据集模拟上游输入，对数据集并发执行
多类型校验规则，结论留痕、可复现、可解释，并支持规则版本化与部分失败重跑。

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
`(dataset_id, rule_id, rule_version, shard)` 为键：

- **可复现**：数据集指纹（行内容的 SHA-256）与规则版本共同决定结论；
  同一数据集在同一规则版本下重跑，结论逐条一致，重复记录相同结论为幂等 no-op。
- **不篡改**：键已存在且既有记录为终态结论（`passed`/`failed`）时，
  写入不一致结论会被拒绝（`DuplicateResultError`）；规则升级（版本 +1）后
  重新校验生成新键，旧版本结论原样保留，可按版本区分查询。
- **非终态可收敛**：`error`/`rejected` 表示"未得出结论"，故障恢复后重跑允许
  追加新结论取代索引；JSONL 文件只追加，历史失败痕迹留痕可查。
- **只重跑失败部分**：`rerun_failed_only=True` 时，已有 `passed` 留痕的
  (规则, 版本, 分片) 直接复用不重复执行；`failed`/`error`/`rejected`/无留痕
  的部分重新执行（`failed` 为确定性结论，重跑结果一致，幂等写入）。

## 并发与分片

`QualityRunner` 用线程池并发执行「规则 × 分片」任务，单任务超时记为
`error` 且不影响其他任务；分片按行号取模稳定切分（可复现）。

> 注意：`duplicate_record` 在分片内检测重复；若需全局去重，请使用
> `num_shards=1`。

## 本地验证方法

```bash
# 运行单测（覆盖并发执行、版本切换、部分失败/超时、可复现性；
# 日志打印输入摘要、规则版本与判定依据）
uv run pytest tests/ -q -s --log-cli-level=INFO

# 启动服务
uv run python main.py   # 或 uv run uvicorn main:app

# 发起校验（示例数据集 data/sample_dataset.json）
curl -X POST localhost:8000/validate -H 'content-type: application/json' -d '{
  "dataset_path": "data/sample_dataset.json",
  "num_shards": 2,
  "rules": [
    {"rule_id": "amount-present", "rule_type": "missing_field", "version": 1, "params": {"field": "amount"}},
    {"rule_id": "amount-range", "rule_type": "value_range", "version": 1, "params": {"field": "amount", "min": 0}},
    {"rule_id": "ts-order", "rule_type": "cross_field", "version": 1, "params": {"left": "created_ts", "right": "updated_ts", "op": "le"}}
  ]
}'

# 按版本查询历史结论
curl 'localhost:8000/results?dataset_id=orders-demo&rule_id=amount-range&rule_version=1'
```

结果存储文件默认为 `./dq_results.jsonl`，可用环境变量 `DQ_STORE_PATH` 覆盖。

## 代码结构

```
dqgate/
  models.py   # 类型定义：规则、结论、终态
  dataset.py  # 本地数据集加载、内容指纹、稳定分片
  rules.py    # 规则定义校验（拒绝非法）与五类规则执行
  store.py    # JSONL 追加式留痕：幂等写、防覆盖、按版本查询
  runner.py   # 线程池并发执行、超时、终态聚合、失败重跑
main.py       # FastAPI 入口：/validate 与 /results
tests/        # 并发、版本切换、部分失败、可复现性测试
```
