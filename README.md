# 边缘推理延时的区间计数与服务凭证仓

机器视觉推理服务的离线覆盖、延时与事件回执。延迟桶为整数毫秒。 资料为本地合成样本。

## 源交付

`sources/` 存放 JSON 原件，文件名前两位是到达序号，同一序号内的各表属于同次交付。每个文件有 `intake`、`table`、`records`，记录 `id` 在所属表内唯一。`source-fields.json` 列出各表字段类型，空值用 `null`，空集合用 `[]`。业务时间按下方约定解释。

| 数据表 | 说明 |
| --- | --- |
| `histograms` | 累计桶计数端点；bounds、counts 各为等长数组。 |
| `coverage` | 流的完整边界和已确证镜像来源。 |
| `policies` | 合同把服务时段分给客户代次。 |
| `offline` | 本地补交范围。complete 为全量交付，supersedes 被明确替换的覆盖。 |
| `requests` | 每份离线范围携带的请求观测。request_key 在 shard 内标识同一请求。 |
| `sent` | 服务台此前实际收到的提交及所用窗口证据。 |
| `receipts` | 服务台对提交的回执及人员响应。 |

## 来源约定

累计直方图 counts 与 bounds 对应，依次覆盖大于前一上界且小于等于当前上界的延时，末桶 inf。累计端点的计数对应到时刻前所处理的请求，跨采集代无可直接相减的连续计数。coverage 指定来源对 shard 的覆盖，gateway 和 edge 可能看到相同请求。覆盖被证实有包含关系时不重复计入；分片号不同的流可互补。offline 完整声明的显式替换区间按指定分片与范围覆盖，不完整的采用已提供请求键作为事件证据。
客户的考核窗口以 policies 的 span 为口径，threshold 毫秒以上算慢，max_ppm 为允许的慢请求百万分比。只在整个证明范围满足时判合规，确证没有服务的空窗口不进入费率分母。响应台以同一客户、服务及相接的已确认越界窗口作为一个连续事件；相邻窗口失去超标依据时按证据影响拆分或撤回，已接收的原事件保留出处。receipt 里 action 需要回到已发请求的证据号。

## 资料盘点

运行环境为 Python 3.11，盘点与资料校验仅使用标准库。盘点结果按到达批次显示表名及行数。

```sh
python source_inventory.py
python -m unittest discover -s tests -v
```

## 延时事实仓 `latency_fact/`

在源交付之上构建的版本化事实仓，把粗/细累计直方图、覆盖声明、离线补交与
服务台回执整理为可按客户、服务、窗口对账的事实表，并保留到证据行的追查链。
全程仅依赖标准库，持久化使用 SQLite（WAL，支持日常整理与历史回查并发）。

### 口径约定（硬约束）

- **区间半开 `[start, end)`**：覆盖、窗口、离线范围同口径；直方图端点 `t`
  的计数对应时刻前已处理请求，同采集代两端点相减得到 `[t0,t1)` 的增量。
- **代内连续才可解释**：跨复位/所有者切换的读数进入“未归属读数”，保留候选
  区间与来源，不进任何窗口权威总量；后来被全量离线证据覆盖时标记化解。
- **镜像不重复计**：`mirror_of` 证实包含关系后只做计数一致性印证，不产证。
- **采样不补全量**：`sample` 明细只抬高慢请求“存在性”下界，不建覆盖、不进
  费率分母；`complete` 离线包按 `supersedes` 撤销旧覆盖权威，禁止重复累加。
- **跨线桶只给区间**：承诺线穿过桶时慢请求保留 `[下界,上界]`，不做均匀假设、
  不平均两种桶的百分位。
- **判定三态加空窗**：`breach`（确实超标）/ `compliant`（确实符合）/
  `insufficient`（证据不足，仅触发补取证）/ `no_service`（确证无服务，不进
  费率分母）。存在开放覆盖缺口时不出具确定性结论。
- **园区正式结论**：任一 shard 未核清则总体结论保留；独立可确认窗口不受牵连。
- **事件与回执**：同客户/服务/shard 的确认越界属同一连续事件，订正按证据影响
  拆分/撤回并保留出处；人工响应独立保留；针对失效证据的旧回执标记陈旧，不能
  关闭新缺口。

### 模块

| 模块 | 职责 |
| --- | --- |
| `model.py` | 区间运算、桶慢请求界限、费率、窗口事实与证据行数据结构 |
| `intake.py` | 源交付装载、结构校验、按到达批次切版本 |
| `coverage.py` | 镜像印证、全量替换、覆盖缺口 |
| `histograms.py` | 代内差分、未归属读数、单调与镜像冲突检测 |
| `requests.py` | 请求键去重、全量/采样分级 |
| `windows.py` | 窗口事实组装、可证界限、合规判定、凭证号 |
| `rollup.py` | 站点/园区汇总与 pending 口径 |
| `events.py` | 连续事件并组、订正链、回执对账、事件台账 |
| `store.py` | SQLite 版本化只追加持久化与查询 |
| `pipeline.py` | 跨版本流水线（窗口独立、事件共享一条订正链） |
| `verify.py` | 需求硬约束的可执行不变量 |
| `cli.py` | 本地命令行入口 |

### 本地运行

```sh
# 从原件重放三个到达批次，落库 latency_fact.db
python -m latency_fact.cli rebuild

# 窗口事实（可加 --version/--customer/--service/--shard）
python -m latency_fact.cli windows --version 3

# 追查任一结论的覆盖片段、计数界限与未归属量
python -m latency_fact.cli evidence --policy k4 --version 3

# 站点/园区汇总（区分无服务时段与未能核清时段）
python -m latency_fact.cli rollup --version 1

# 事件提案订正链与平台回执对账
python -m latency_fact.cli events
python -m latency_fact.cli receipts

# 导出某版本窗口事实 JSON；内置不变量验证
python -m latency_fact.cli export --version 3
python -m latency_fact.cli verify
```

### 验证

`tests/` 下 53 个用例覆盖纯函数口径、端到端三版本事实、异常输入（镜像冲突、
累计倒退、跨代桶变化、覆盖外读数）、SQLite 并发读写与凭证稳定性：

```sh
python -m unittest discover -s tests -v
```

每个窗口事实带稳定凭证号 `wf-<sha256前12位>`，摘要纳入版本、窗口、判定与
可证界限，重复构建不漂移、跨版本可区分，保证一份在用凭证不会混入新口径或旧计数。

