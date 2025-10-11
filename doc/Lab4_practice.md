# Lab 4: SimpleDB Transactions

Task: Implement a simple locking-based transaction system in SimpleDB.

## 1. Transactions, Locking, and Concurrency Control

### 1.1 Transactions
事务 = 一组数据库操作（插入、删除、读取等）组成的逻辑整体，这些操作要么全部执行成功，要么全部不执行。

对外表现是“要么什么都没发生，要么整个事务都完成了”，不能有中间状态。

### 1.2 ACID Properties
- **Atomicity (原子性)**: 事务中的所有操作要么全部完成，要么全部不完成。
  - 靠 Strict 2PL(two-phase locking) + 缓冲区管理 实现
- **Consistency (一致性)**: 事务执行前后，数据库必须保持一致状态。
  - simpleDb仅有事务一致性（由原子性保证），其余一致性问题(e.g., key constraints)没有得到解决。
- **Isolation (隔离性)**: 并发执行的事务之间互不干扰，每个事务的中间状态对其他事务不可见。
  - 靠 Strict 2PL 实现
- **Durability (持久性)**: 一旦事务提交，其结果是永久性的，即使系统崩溃也不会丢失。
  - 使用 FORCE 策略提交后立即写盘

**什么事么是 Strict 2PL？**
Two-Phase Locking（2PL） 是一种加锁协议，规定事务在执行时必须分为「先只加锁」再「只解锁」两个阶段，从而保证事务调度的可串行化和隔离性。
- 增长阶段（growing phase）：事务可以加锁，但不能释放锁。
- 收缩阶段（shrinking phase）：事务可以释放锁，但不能再加锁。

**严格的 2PL (Strict 2PL)**: 事务在提交前不释放任何锁，提交后一次性释放所有锁。
- **好处**: 保证了可串行化调度和隔离性，并简化了恢复过程（因为未提交事务的修改不会被其他事务看到）。
- **坏处**: 可能导致死锁，需要死锁检测机制。

### 1.3 Recovery and Buffer Management

- **NO STEAL 与 STEAL 的含义**
  - NO STEAL: 事务未提交前，不能将其修改过的页从缓冲区写回磁盘。
    - **好处**：崩溃时磁盘上不可能有未提交事务的脏页，因此不需要 UNDO。
    - **坏处**：缓冲区压力大，可能导致内存不够（因为不能把这些页换出去）。
  - STEAL: 即使事务没提交，也可以把脏页换出到磁盘。
    - **好处**：缓冲区利用率高。
    - **坏处**：崩溃时磁盘可能含有未提交事务的数据，需要 UNDO 日志 来回滚。
- **FORCE 与 NO FORCE 的含义**
  - FORCE: 事务提交时，强制将其修改过的页写回磁盘。
    - **好处**：提交后磁盘数据已是最新，崩溃后不需要 REDO。
    - **坏处**：提交延迟大，I/O 成本高。
  - NO FORCE: 事务提交时，不强制将其修改过的页写回磁盘。
    - **好处**：性能更好（可以延迟和合并写入）。
    - **坏处**：崩溃后磁盘可能没最新数据，需要 REDO 日志 来重做已提交事务。

工业级DBMS（如 PostgreSQL、MySQL）用的策略是 STEAL + NO FORCE，因为它们需要高性能和高并发。它们额外实现了复杂的 WAL（Write-Ahead Logging） 恢复系统。

### 1.4 Granting Locks

需要一个数据结构来记录：
- 哪些事务持有哪些锁
- 每个数据项（Page 或 Tuple）被谁锁住了，锁的类型是共享锁（S-Shared）还是排他锁（X-Exclusive）-> 检查是否能够授予锁

#### 锁的规则
1. 读前必须拿 S 锁
  - 多个事务可以同时读同一对象，不互相阻塞
2. 写前必须拿 X 锁
  - 只有一个事务可以对一个对象具有排他锁
3. S 锁可共享，X 锁独占
  - 已有 S：允许再来 S；不允许 X
  - 已有 X：S/X 都不允许
4. S 锁可以升级为 X 锁（一种机制，在你仍然持有 S 的时候，向锁管理器请求把它“提升”为 X，而不释放锁）
  - 若事务 t 是唯一持有该对象 S 锁的事务，可把自己的 S 升级为 X（避免先释放再申请期间被别人插队）
  - 若还有别的读者在持有 S，则需要等待它们释放S后才能完成升级。
  - **如果有两个S都想升级呢？** -> 可能死锁
    - 解决方案 1：只有当事务本身就是唯一持有者时，才允许它升级；否则必须先释放 S 再申请 X，但会有插队风险。
    - 解决方案 2：允许升级，但引入死锁检测机制，一旦发现死锁，就选择牺牲者（victim），强制回滚其中一个事务，释放它的锁。
    - 解决方案 3：升级优先策略 + 阻止新读者进入。一旦有事务请求升级，阻止新的 S 锁进入，优先清场现有读者。
5. 不可立即授予就阻塞
  - 请求锁时，如果与当前持有者冲突，应阻塞等待直到锁可用（其他线程释放后再唤醒）
  - 阻塞 -> 唤醒 需要注意 race condition 问题（当多个线程或进程“竞争”访问共享资源时，由于执行顺序不可预测，导致程序行为变得不确定甚至错误，这就叫 race condition）

#### 锁为什么这么设计
- 让“读-读”并发最大化（S 可共享），同时保证任何“涉及写”的情况都串行化（X 独占），从而满足可串行化调度与隔离性。
- S→X 升级避免“先放再抢”的窗口竞态，减少不必要的阻塞与饥饿。

#### Exercise 1
仅使用Page级别的锁
*  Modify <tt>getPage()</tt> to block and acquire the desired lock
   before returning a page.
*  Implement <tt>unsafeReleasePage()</tt>.  This method is primarily used
   for testing, and at the end of transactions.
*  Implement <tt>holdsLock()</tt> so that logic in Exercise 2 can
   determine whether a page is already locked by a transaction.

##### 设计一个 LockManager 类，负责记录“谁（Transaction）在什么资源（Page）上持有什么锁（S/X）”，并决定能不能给下一个人锁。
- 设计目标：
    - 粒度：Page 级 S/X 锁（共享/独占）
    - 能力：阻塞等待、读-读并发、升级（S→X）、可重入（同一事务已拿到 X，再读允许）
    - 简化：不做死锁检测、不谈公平性/超时、不做锁计数（同一事务多次获取 S 只记一次，由于Strict 2PL）
    - 用例：配合 SimpleDB 的 strict 2PL，事务结束 releaseAll 统一释放
- 数据结构：
    - 两张表（双向映射）：
        1) page -> state（当前Page的锁状态）Map<PageId, LockState> lockTable;
        2) tid -> pages（这个事务持有哪些页）Map<TransactionId, Set<PageId>> transactionLocks;
    - 每个 Page 的锁状态 LockState类
        - LockType type; // 当前锁类型：NONE, S, X
        - Set<TransactionId> holders; // 当前持有锁的事务集合
- “能否授予锁”的规则 （只需要state，request和transactionId）
  - 没人持锁 -> 可授予
  - 请求 S:
    - 当前是 S -> 可授予（共享）
    - 当前是 X，但持有者是自己（state.holders.contains(tid)当前锁是自己持有的） -> 允许 在 X 下再次读（可重入读）-> 可授予（可重入）
    - 否则阻塞
  - 请求 X:
    - 当前是 S:
      - 只有自己持有 S -> 升级为 X -> 可授予（S→X 升级）
    - 当前是 X:
      - 持有者是自己 -> 可重入写 -> 可授予
    - 否则阻塞
- 实现acquire获取锁
  - 目标：
    - 判断需要哪种锁（读要 S，写要 X）
    - 检查是否能授予，如果现在不能给，就阻塞等待（直到别人释放）
    - 一旦能给，就把“锁状态”和“谁持有了它”记录下来
  - 需要的参数：
    - 谁来申请锁 TransactionId tid
    - 申请哪个资源 PageId pid
    - 读还是写 Permissions perm
      - 调用者（BufferPool）只知道读/写的权限（Permissions），不知道你内部用 S/X
      - 所以还需要一个转换函数（翻译）：perm -> LockType
  - Step1: 把 Permissions 翻译成锁类型
  - Step2: 找到这页当前的锁状态
  - Step3: 检查能否授予
    - 能 -> 更新状态，记录持有者
    - 不能 -> 阻塞等待（wait）
      - 等待时要用 while 循环检查条件（防止虚假唤醒）
  - Step4: 更新事务持有的页集合
- 实现release释放锁
  - 目标：
    - 让事务 tid 不再持有 页面 pid 的锁
    - 正确更新页级状态（LockState.type 和 holders）
    - 正确更新反向索引（transactionLocks 里删掉这个 pid）
    - 唤醒(notifyAll)等待中的 acquire（让他们有机会继续尝试拿锁）
  - 需要的参数：
    - 谁要释放 TransactionId tid
    - 释放哪个资源 PageId pid
  - Step1: 找到这页当前的锁状态
  - Step2: 检查 tid 是否真的持有这个锁
  - Step3: 更新状态
    - 从 holders 里删掉 tid
    - 如果 holders 变空，没人再持锁 → 设为空闲
    - 如果 holders 还剩人 且 当前是共享锁 → 继续保持 SHARED
    - holders 还不空 但 type == EXCLUSIVE（理论上不该发生）→ 设为 SHARED（降级）
  - Step4: 更新反向索引，从 transactionLocks 里删掉这个 pid
    - 先拿到该事务当前的页集合
    - 只把这次释放的那一页移除
    - 只有当这个集合被删空（说明这个事务已经不再持有任何页的锁）
      才把这条「事务 → 集合」的映射整个删掉
  - Step5: 唤醒等待的事务
- 实现releaseAll 释放“一个事务的所有”锁
  - 目标：
    - 一次性把事务 tid 持有的所有页面的锁都释放
    - 并把锁表和反向索引清理干净
    - 最后唤醒等待者，让他们有机会继续拿锁
  - 需要的参数：
    - 谁要释放 TransactionId tid
  - Step1: 一次性取出要释放的所有页
  - Step2: 逐页释放（直接改 lockTable）
    - LockState state = lockTable.get(pid);
    - state.holders.remove(tid);
    - 如果 holders 空 → state.type = null（空闲）
    - 否则如果 state.type == EXCLUSIVE → 降级为 SHARED
  - Step3: 唤醒等待的事务
- 实现holdsLock

##### BufferPool需要完成什么
1. 在返回 Page 前先拿到正确的锁，getPage是所有访问页面的唯一入口
   - 读 → 共享锁（S）
   - 写 → 独占锁（X）
   - 拿不到就阻塞等待，直到能拿
2. 在需要时释放锁
    - 释放单个页的锁（测试/调度用）：unsafeReleasePage(tid, pid)
    - 查询是否已持锁：holdsLock(tid, pid)
3. 事务结束时一次性释放所有锁 (transactionComplete)

### 1.5 Lock Lifetime 怎么让锁“活多久” —— 锁什么时候加、什么时候释放

#### 实现 strict 2PL（严格两阶段锁协议）
2PL = 加锁阶段 + 解锁阶段
strict = 所有锁要等到事务结束（commit/abort）之后才释放
所以：事务在访问某个对象（页面或元组）之前，必须先获得合适类型的锁（S 或 X），并且在事务提交或中止之前，不能释放任何锁。

#### 推荐的加锁时机 getPage()
所有访问页面的地方都经过 getPage()

只要 getPage() 里先调用 lockManager.acquire()，就保证了所有访问都自动带锁。

#### 确认 HeapFile 和 BufferPool 使用正确的权限
检查它们传进去的 Permissions 是否正确：
- 遍历（SeqScan）：Permissions.READ_ONLY
- 插入 / 删除：Permissions.READ_WRITE

#### 对Strict 2PL的优化 - 局部放锁
**允许： 对不再访问、不影响结果的对象提前解锁。**
例如：插入时扫描所有页。
- 在第一个、第二个、第三个页都没空位时，锁可以提前释放（因为已经确定要插入到后面的页了，不会再访问前面的页）。
- 可以释放的原因：
  - 你不会修改；
  - 你只是读取“是否有空”这一状态；
  - 并且你不再依赖这些页的信息来做之后的决策 / 未来即使被别的事务改了，也不会影响你后续行为

#### Exercise 2
Exercise1 基本上已经实现了所有功能，只需要加一个局部放锁 -> HeapFile.insertTuple()

1. 扫描“已有页”：
  - 只拿 S（READ_ONLY） 锁看一眼空槽；
  - 不合适就立刻释放（局部放锁），不要占着锁阻塞别人。
2. 如果某页看起来有空：
  - 升级到 X（READ_WRITE） 再真正插；
  - 升级成功后再检查一次空槽（防止抢不到）；
  - 成功插入：保持这页的 X 锁直到事务结束（不要在这里释放）。
3. 遍历完没有空：
  - 原子地在文件尾追加一页（避免多线程重复追加/页号竞争）；
  - 对新页持 X 锁插入；
  - 保持这页的 X 锁到事务结束。

为保证原子性，需要一个分配锁（allocation lock）。
- LockManager 负责“逻辑锁”（谁能读写某页）
- HeapFile 自己的 allocMutex 负责“物理锁”（谁能往文件尾加页）

### 1.6 Implementing NO STEAL

#### 什么是 NO STEAL
没提交的事务的改动，不能写到磁盘（意味着不能evict脏页）。

如果内存里全是脏页 → 没法驱逐任何一个 → 抛异常（DbException）。

只驱逐干净页

#### 一般公司都会用 STEAL
即使事务未提交，也允许将脏页写回磁盘（或驱逐）。

但需要复杂的日志和恢复机制（UNDO/REDO）来保证原子性和持久性。

#### Exercise 3
在 BufferPool.evictPage() 里实现 NO STEAL 策略

### 1.7 Transactions
#### Exercise 4
实现 BufferPool里的两个transactionComplete() 来完成事务
1.	transactionComplete(TransactionId tid) → 默认提交，可以直接调用 transactionComplete(tid, true)
2.	transactionComplete(TransactionId tid, boolean commit) → 根据 commit 参数决定是提交还是回滚

实现逻辑：
```text
transactionComplete(tid, commit)
  ├─ 如果 commit == true：
  │    - 找出所有 dirty pages
  │    - 把它们写回磁盘（flush）
  │    - 标记干净
  │
  ├─ 否则（abort）：
  │    - 找出所有 dirty pages
  │    - 重新从磁盘读取旧版本覆盖缓存（回滚）
  │
  └─ 最后（无论如何）：
       - 释放该事务持有的所有锁
```

### 1.8 Deadlocks and Aborts

#### 检测死锁的方法
1. 超时法（Timeout Policy）

最简单的方式：如果一个事务等待太久，就直接中止它。

2. 依赖图循环检测（Wait-For Graph）

**建立一个 “等待依赖图（wait-for graph）”：**
- 图中每个节点是一个事务
- 如果事务 T1 等待 T2 释放某个锁，就画一条边 T1 → T2
- 每次申请新锁时，检查图中是否形成了一个环（cycle）
- 如果有环 → 死锁。此时需要中止至少一个事务

**检测到死锁：**
- 中止当前事务，或
- 中止被等待的事务

3. 基于时间戳的预防方案（WAIT-DIE / WOUND-WAIT）

为每个事务分配一个全局时间戳（开始时刻）：
- 小时间戳 = 年长事务（older）
- 大时间戳 = 年轻事务（younger）

通过比较时间戳，控制谁能等待、谁必须被中止，从而避免形成环。

**WAIT-DIE：**
- 老事务请求被年轻事务持有的锁 → 允许等待（wait）
- 年轻事务请求被老事务持有的锁 → 直接中止（die）

**WOUND（伤害）-WAIT：策略更激进**
- 老事务请求被年轻事务持有的锁 → 强制中止年轻事务（wound）
- 年轻事务请求被老事务持有的锁 → 允许等待（wait）

#### Exercise 5
1. 实现死锁检测或预防机制（任选其一）；
2. 在检测到死锁时，抛出 TransactionAbortedException；
3. 不要自动重启事务，测试代码会帮你调用 transactionComplete()；
4. 测试文件：DeadlockTest.java
  - 会创建多个事务模拟死锁；
  - 如果程序挂起不动（死锁未检测出来），测试会超时失败；
  - 如果你实现正确，会抛出 TransactionAbortedException 并通过测试。

**实现思路（用wait-for graph）：**
1. 在哪改？

LockManager.acquire() 里进行修改，因为那里是所有锁请求的入口。

2. 怎么改？

在acquire里：
- 计算能否授予；
- 不能授予 → 在等待图登记“我等着谁”
  - 检测是否形成环 
  - 有环 → 清理登记并抛出 TransactionAbortedException
  - 无环 → 正常阻塞等待 wait()
- 成功授予后 → 清掉我 tid 相关的等待边
- notifyAll() 唤醒等待者

3. 因此需要加的数据结构

等待图：tid -> 我当前在等待的那些事务（有向边）：
- Map<TransactionId, Set<TransactionId>> waitFor;
  - key: 等待者 tid
  - value: 被等待者 tid 集合

正在等待哪个Page（便于释放/清理）：
- Map<TransactionId, PageId> waitingOn

4. 怎么画等待图（加边）？

若 want = EXCLUSIVE:
- 当前 type = SHARED: 
  - 持有S的只有自己（升级）： 不指向任何人
  - 持有S的有别人： 当前tid指向所有持有S锁的事务（除自己）
- 当前 type = EXCLUSIVE: 
  - 若当前tid是持有X锁的事务（可重入写）: 不指向任何人
  - 否则: 当前tid指向持有X锁的事务

若 want = SHARED:
- 当前 type = EXCLUSIVE: 当前tid指向持有X锁的事务

5. 怎么检测环？

从当前 tid 出发做 DFS，看能否回到自己

**具体实现**

- Step1: 在LockerManager里加上等待图的数据结构
- Step2: 新写一个addWaitEdges函数实现这个加边逻辑（登记）
- Step3: 新写一个hasCycle函数实现环检测（DFS）（检测）
- Step4: 新写一个清理登记的clearWaitingOf函数（清理）
- Step5: 在acquire里调用这三个函数
- Step6: 在release里实现入边的清理，**因为release释放的是tid自己的锁的状态，变成None了，但是tid还可能在等待变成别的锁**
- Step7:在releaseAll里实现clearWaitingOf(tid)（因为releaseAll是事务结束时调用的，必须清理掉所有等待边）


