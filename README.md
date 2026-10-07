# Deadlock Simulator Backend

模拟作业—单实例资源系统中的授予/完成/死锁解除过程（至多 12 个作业、15 个资源）。

## 语义

- 每个资源至多由一个作业持有；每个作业可持有多个资源，同一时刻最多等待一个资源。
- 模拟器反复执行直至不能前进：
  1. **完成**：所有不再等待的作业完成并释放所持资源（按作业 ID 升序）；
  2. **授予**：每个有空闲等待者的资源授予**作业 ID 最小**的等待者（按资源 ID 升序应用）。
- 若仍有作业卡住（死锁），枚举可中止作业子集并独立重放，选出**总中止代价最小**
  且能使剩余作业全部完成的集合；代价并列时取**排序后中止 ID 序列字典序最小**者。
- 受保护（不可中止）作业导致无解时，返回 `unresolvable` 并明确报告，
  绝不强行释放受保护作业的资源。

## 文件

- `deadlock_simulator.py` — 核心：`build_state`（校验，`allow_choices=True` 时接受
  `waiting_any`）、`simulate`（授予/完成回放，候选资源在同一授予阶段动态裁决）、
  `find_min_abort_set`（从真实停滞状态做子集枚举 + 独立重放）、`solve`（完整求解）。
- `resource_choices.py` — `solve_choices(payload)`：候选资源（`waiting_any`）入口，
  直接调用 `solve(payload, allow_choices=True)`，不改写输入。
- `backend_server.py` — 标准库 HTTP 后端：`POST /simulate`、`POST /simulate/choices`、`GET /health`。
- `test_deadlock_simulator.py` — unittest 测试（无需第三方依赖）。

## 运行

```bash
python3 backend_server.py 8000        # 启动后端
python3 -m unittest test_deadlock_simulator -v   # 运行测试
```

## API

`POST /simulate`，请求体：

```json
{
  "jobs": [{"id": 1, "holding": [1], "waiting_for": 2, "abortable": true, "abort_cost": 5}],
  "resources": [{"id": 1, "holder": 1}, {"id": 2, "holder": null}]
}
```

响应 `status` 为 `completed` / `resolved_with_aborts` / `unresolvable`，
`events` 为 `grant` / `complete` / `abort` 事件的完整有序回放；
非法输入返回 400 及错误说明。



## 替代资源等待
`POST /simulate/choices` 允许每个作业使用 `waiting_any`（非空资源 ID 数组）替代 `waiting_for`，任取一项即可继续。未使用替代数组的作业沿用旧语义；`waiting_for` 与 `waiting_any` 不允许同时指定（`POST /simulate` 完全不接受 `waiting_any`）。完成阶段按原规则释放（只要本轮有完成就重复完成阶段）；授予阶段按资源 ID 升序、针对**当前**等待者动态裁决——某资源授予最小作业 ID 后，该作业立即退出同阶段其他候选资源的等待者集合，因此每个等待作业在一个授予阶段至多取得一个资源，每轮完整授予后再进入完成阶段。陷入停滞时，从实际停滞状态独立重放各中止子集，仍按总代价最小、中止 ID 序列字典序最小全局择优；无解时返回 `unresolvable` 且不做部分中止。每条实际授予及释放必须能逐步重放，受保护作业不能被中止；候选重复、未知、已持有、空数组或类型非法时整次请求以 400 拒绝，输入载荷在任何情况下都不被改写。
