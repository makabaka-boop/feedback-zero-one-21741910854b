# 双分库两阶段提交（2PC）实现

纯 Python 标准库 + SQLite 实现的协调者/分库 2PC 系统。三个角色各自是**独立进程、
独立 SQLite 数据库**，仅经 HTTP 通信。目标：实验数据向双分库发布时**绝不暴露半批**，
并在断连、丢响应、重复投递、进程强退等故障下保证终态一致。

## 目录

| 文件 | 作用 |
|---|---|
| `common.py` | 输入校验（txid/key 标识规则、写集规范化、epoch 正整数校验）、HTTP 客户端工具（可携带 epoch） |
| `shard.py` | 分库（参与者）进程：prepare/commit/abort、键锁、持久终态、故障注入、代安装与代号守卫 |
| `coordinator.py` | 协调者进程：事务登记、2PC 驱动、持久决定、重启恢复、一致查询、fenced 代（epoch）领号与接管 |
| `acceptance_test.py` | 三进程端到端验收（含两类强退与三类通信故障） |
| `fence_acceptance_test.py` | fenced 代模式验收：双协调进程滚动接管、强退、迟到代消息、线性一致（99 项断言） |
| `run_demo.sh` | 手动启动三进程演示（`./run_demo.sh` 普通模式 / `./run_demo.sh fenced` 代模式） |

运行要求：Python 3.8+，无第三方依赖。所有命令建议用 `python3 -I` 运行。

## 启动

```bash
# 一键验收（自动拉起/强退/重启三个进程，结束自动清理）
python3 -I acceptance_test.py        # 普通模式，88 项断言
python3 -I fence_acceptance_test.py  # fenced 代模式，99 项断言

# 手动演示
./run_demo.sh            # 普通模式
./run_demo.sh fenced     # fenced 代模式
```

## 事务 JSON 格式

```json
{
  "txid": "tx001",
  "writes": {
    "db1": {"k1": "a", "k2": null},
    "db2": {"k1": null}
  }
}
```

* `txid`：非空 ASCII 字母数字串；`writes` **恰含 `db1`、`db2` 两个库**。
* 每库是 key→字符串或 null 的映射；key 同为非空 ASCII 字母数字串。
* 写集可以为空 `{}`（空写集合法）；写入语义为新增或替换，`null` 也是显式值
  （与键不存在相区别）。初始两库均为空。
* 非法输入返回 `400` 且错误信息定位到具体字段（如 `writes.db1.k: ...`），**不建事务**。

## HTTP 接口

### 协调者（默认 19010）

| 方法/路径 | 说明 |
|---|---|
| `POST /submit` | 提交事务；同步返回 `{"state":"COMMITTED|ABORTED"}`，仅双方确认 commit 后才 committed |
| `GET /txn/{txid}/status` | 协调者侧事务状态 `PENDING/COMMITTED/ABORTED` |
| `GET /data` | **双库一致查询**：等待在途事务结束，并在提交互斥锁内读取两库，按 key 字节序返回 |
| `GET /debug/epoch` | 观测：`fenced`、本进程 `epoch`、代账最大值 `ledger_max`、是否 `fenced_out`（无代号守卫） |
| `POST /control/crash-after-first-prepare` | 故障注入：下一个事务在单库 prepare 后 `SIGKILL` 式强退 |
| `POST /control/crash-after-first-commit` | 故障注入：下一个事务 commit 落盘且仅一库应用后强退 |

### 分库（默认 19011/19012）

| 方法/路径 | 说明 |
|---|---|
| `POST /install` | fenced 专用：安装代 `{"epoch":N}`；同代幂等、低代 409 拒绝、高代原子提升；不动准备/锁/终态 |
| `POST /prepare` | 持久写集并独占键锁；锁冲突立即投反对票（不等待）；不改可见数据 |
| `POST /commit` | 仅 PREPARED 可提交；原子应用写集、释放锁；重复调用幂等 |
| `POST /abort` | 丢弃准备并释放锁；**先于 prepare 到达也持久记 ABORTED** |
| `GET /txn/{txid}/status` | 分库侧状态 `PREPARED/COMMITTED/ABORTED` |
| `GET /data` | 仅已提交数据，按 key 字节序（fenced 下须带代） |
| `GET /debug/epoch` | 观测：分库是否 fenced 与已安装代（无代号守卫） |
| `GET /debug/locks`、`/debug/stats` | 持锁视图、应用计数（验收幂等用） |
| `POST /control/drop-next` | `{"phase":"prepare|commit|abort"}`：下一次该接口落盘后丢弃应答 |
| `POST /control/delay-next` | `{"phase":...,"seconds":N}`：下一次该接口落盘后延迟应答（制造接管竞态） |

## 协议与正确性要点

1. **先持久登记，再通信**：协调者 `/submit` 先把事务写入本地 SQLite（PENDING），
   之后才发起 prepare。
2. **交错 prepare + 键锁**：分库把写集和 `(txid,key)` 锁持久化；任一键已被其他
   事务持有就立即返回反对（本库同时持久 ABORTED），不阻塞等待。PREPARED 期间
   数据对外不可见。
3. **内容固定**：同 txid 的写集以首次内容为准（规范化 JSON 落盘），异内容一律
   `409` 拒绝，且**内容判断先于锁冲突判断**；协调者侧重提异内容同样直接拒绝。
4. **决定持久且不可逆**：全赞成才落盘 COMMIT，任一反对落盘 ABORT；未收齐应答
   可无限等待/重试，**从不以超时推翻决定**。决定落盘后重发通知，分库幂等应答。
5. **终态语义**：分库 COMMITTED/ABORTED 永不反转；abort 先到时持久 ABORTED，
   迟到 prepare 返回原终态、不复活；重复 prepare/commit/abort 全部幂等。
6. **协调者重启恢复**（同步完成后才对外服务）：
   * PENDING（无决定）→ 补 ABORT 决定并通知双方撤销；
   * COMMITTED → 继续补发 commit 直到双方确认（半批终将补齐，期间 `/data`
     不对外提供，避免读到半批）；
   * ABORTED → 补发 abort。
7. **一致查询串行等价**：`/data` 先等待所有在途工作线程结束，再在全局提交互斥锁内
   顺序读两库。commit 对两库的发布也在同一把锁内，因此查询要么看到整批、要么
   整批都看不到；发布未完成时查询等待。
8. **通信故障**：断连、丢响应、重复投递均靠“持久状态 + 幂等重试”收敛。分库用
   `stats.commit_applied` 计数证明同号重试不会重复应用写集。

## fenced（代 / epoch）模式

滚动接管“双分库发布”时，旧协调进程可能仍存活。给**全新集群**可选启用 fenced
模式（协调者与分库均加 `--fenced`），用单调代（epoch） fencing 掉旧代，保证
任何时刻只有当前代能登记事务或持久决定。未启用时协议字节与行为完全不变。

### 代的产生与持久化

* **共享协调库 + 独立代身份**：多个协调进程共享同一个协调库，其中
  `epoch_ledger` 是跨进程代账。代号由 `BEGIN IMMEDIATE` 写事务内的
  `max(epoch)+1` 原子插入产生，**从 1 递增、只增不复用**；实测 8 进程并发领号
  严格得到 1..8 不重号。每个协调进程的当前代身份另存独立文件
  （`--epoch-file`，各进程必须不同；同进程普通重启沿用该身份、不重复领号）。
* **分库持久最大代**：每个分库在 `meta` 表保存已安装的最大代。
* 存储采用 `DELETE` 回滚日志 + `synchronous=FULL` + `busy_timeout`：在容器
  overlayfs 等映射文件系统上，WAL 的共享内存在跨进程并发首启时不可靠；
  DELETE 模式靠文件锁串行化写者，对本低负载实验语义最稳。

### 启动接管序列（全部完成前不开放 HTTP）

1. **原子领号**：全新进程从代账领取下一个代；同身份重启沿用原代。
2. **先向两个分库安装本代**（`POST /install`）：同代幂等、低代 409 拒绝、
   高代原子提升。**安装绝不清除准备、键锁或终态**。
3. **再恢复旧事务**：沿用原 txid 与内容——已有决定（COMMIT/ABORT）继续向双方
   补发通知；无决定（PENDING）补 ABORT 并撤销。
4. **全部恢复完成**后才开始接受提交与一致查询。

### 协议代号规则

* fenced 下 `prepare/commit/abort` 与 `/data` 都必须携带已安装代，支持
  query（`?epoch=N`）或 body 字段。
* **校验顺序：先类型，后代号，再内容/锁/终态**：
  * 代必须是**正整数**，明确禁止布尔（`true/false` 是 int 子类）、浮点、
    字符串、零与负数，非法返回 `400` 且错误定位到 `epoch`；
  * 代号与分库已安装代不符（含未携带、分库尚未安装）返回
    `409 {"reason":"stale_epoch",...}`，**不改变任何状态**；
  * 代号通过后才沿用既有的内容固定、键锁、终态不可逆等规则。
* 协调者入口在代号匹配后还核对共享代账最大值：一旦代账出现更新代，旧进程
  即视为失效，其**新收到的客户端请求一律 409**。

### 旧代失效与决定安全

* 协调进程在任何分库响应中收到 `stale_epoch`，立即**自我 fence-out**：停止
  一切写动作，进行中的工作线程中止，不再登记事务或持久决定。
* **`stale_epoch` 不是反对票**：旧代的任何响应或错误都不能促成新决定；被
  fence 的事务保持其原有状态，交由新代按接管规则处理。
* 落任何决定前再核对代账最大值，只有当前代（最大值保持者）能持久决定。

### 典型示例（`fence_acceptance_test.py` F5）

第 1 代事务 T 向两库写 `k="a"`，已决定提交且仅 db1 应用（db2 仍 PREPARED
持锁）。第 2 代接管：领到更高代并安装两库 → 恢复时沿用原 txid 与内容，把
commit 补发给 db2，最终**两库 k 均为 "a"**，db1 不重复应用；此后迟到的
第 1 代 `abort` 携带旧代号，被两库以 `409 stale_epoch` 拒绝，终态与数据不变。

### fenced 验收场景（`fence_acceptance_test.py`，99 项断言）

| 编号 | 覆盖点 |
|---|---|
| F1 | 全新集群：领号=1、两库安装、未安装前协议请求 409 且不留痕、整批提交/查询 |
| F2 | 代协议：类型先于代号；禁布尔/浮点/零/负；缺失/错代 409 不改状态；安装幂等与低代拒绝 |
| F3 | **只安装一库时再次强退**：重启幂等安装、不领新号不跳号、安装不清数据 |
| F4 | **无决定撤销**：两库 PREPARED 持锁的 PENDING 被新代撤销，值不可见、锁释放 |
| F5 | 题面示例：半批接管补齐双库、迟到旧代 abort 拒绝、终态/锁/应用次数核对 |
| F6 | **迟到 prepare 与提交应答丢失**：旧代 prepare 409 不留痕；丢应答重试只应用一次 |
| F7 | **旧协调进程仍存活**：新代安装后旧代请求全 409；在途 worker 撞 stale 自我 fence-out，stale 不促成决定 |
| F8 | **线性一致**：接管窗口错代查询 409 绝无半批；代账 1..N 连续不复用；终态不可反转 |

共 99 项断言，可重复运行（每轮清空 `run_fence/` 重建进程与库）。

## 验收场景（`acceptance_test.py`）

1. 正常整批提交：同批 `k="a"` 与 `k=null` 两值同时保留；替换/新增；空写集；字节序
2. 持键冲突：交错事务抢键立即反对、整批 abort、锁释放后同键可再提交
3. 撤销先到：abort 早于 prepare 持久终态，迟到 prepare 不复活，终态不可反转
4. prepare/commit 应答丢失：重试收敛，`commit_applied` 计数证明只应用一次
5. **强退点 A**：单库 prepare 后强退协调者 → 重启后撤销、准备值不可见、锁释放、
   同号重提不复活
6. **强退点 B**：commit 落盘仅一库应用后强退 → 重启补齐双库、半批不暴露、
   同号重试不重复应用、异内容重提 409
7. 同 txid 同 JSON 重提返回原状态；异内容先于锁冲突被拒
8. 15 组非法输入逐字段定位且不建事务
9. 多事务交错提交、分库重启保留身份/结局/数据
10. 分库断连：宕机期间协调者保持 PENDING 不以超时作决定，恢复后整批提交完成

共 88 项断言，可重复运行（每轮清空 `run_acceptance/` 重建进程与库）。
