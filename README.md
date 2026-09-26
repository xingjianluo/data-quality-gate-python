# Data Quality Gate (Python)

本地数据质量校验与监控服务：用本地数据集（`data/*.json`）模拟上游输入，
对数据执行版本化规则校验，结论追加留痕，支持并发执行、部分失败与只重跑失败部分。

## 规则模型

每条规则为 `(rule_id, rule_type, version, params)` 四元组，同一 `rule_id` 下
`version` 唯一标识一份定义。支持的规则类型：

| rule_type          | 说明         | 关键参数 |
|--------------------|--------------|----------|
| `missing_field`    | 字段缺失     | `field` |
| `value_range`      | 取值越界     | `field`, `min`/`max`（至少其一） |
| `duplicate_record` | 重复记录     | `keys`（联合键列表） |
| `freshness`        | 数据新鲜度   | `field`, `max_age_seconds`, 可选 `reference_time` |
| `cross_field`      | 跨字段一致性 | `left`, `op`（eq/ne/gt/ge/lt/le）, `right` |

单条规则的执行状态（`CheckStatus`）：

- `passed` / `failed`：数据满足 / 不满足规则；
- `rejected`：规则定义非法（如缺参数、`min > max`），**拒绝执行**，原因可区分；
- `error`：规则执行异常或超时，**显式留痕**，绝不静默跳过或默认通过。

新鲜度规则的参考时间取参数 `reference_time`，缺省时取数据内最大时间戳——
不使用 wall-clock，保证同数据集 + 同规则版本的结论可复现。

## 版本与重跑语义

- 每次运行的所有结论以 JSONL **只追加**写入 `data/results.jsonl`（路径可用
  `DQ_STORE_PATH` 覆盖），历史结论永不被篡改或覆盖；
- 规则更新 = 提升 `version` 后重新校验，新旧版本结论并存，可按
  `rule_version` 区分查询（`GET /checks/results?dataset_id=..&rule_id=..&rule_version=..`）；
- `run_id` 为输入内容哈希（数据集指纹 + 规则规格），同输入必同记录；
- 部分重跑：`POST /checks/run` 传 `only_rule_ids`，仅重跑集合内规则，
  其余规则结论从最近一次同数据集指纹的运行中**复用**，不重复执行已成功部分；
  数据集指纹变化时拒绝部分重跑（409）。

## 终态定义（Verdict）

整体终态由全部规则状态确定，**部分失败不会静默变为通过**：

| 条件 | 终态 |
|------|------|
| 任一规则 `error` / `rejected` | `partial`（未完整评估，不下通过/失败结论） |
| 否则任一规则 `failed` | `failed` |
| 全部 `passed` | `passed` |

## 并发与超时

同一数据集的多条规则在 `ThreadPoolExecutor` 中并行执行，结果互不污染
（按 `rule_id` 排序后落痕）；单规则超时映射为 `error` 并立即给出结论，
执行器以 `shutdown(wait=False)` 关闭，运行不会被挂住。

## 本地验证

```bash
uv sync                              # 安装依赖
uv run pytest tests/ -q              # 运行全部测试（并发/版本切换/部分失败）
uv run pytest tests/ -q -s --log-cli-level=INFO   # 查看输入摘要、规则版本与判定依据日志
uv run uvicorn main:app              # 启动服务
```

API：

- `POST /checks/run`：`{dataset_id, rules[], only_rule_ids?}`，执行并留痕；
- `GET /checks/results`：按 `dataset_id` / `rule_id` / `rule_version` 查询历史结论。

示例数据集 `data/orders.json` 含越界金额、重复 `order_id`、缺失 `user`、
过期 `updated_at`，可直接触发各类失败路径。
