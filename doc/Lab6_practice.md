# Lab 6: Rollback and Recovery

## 0. Getting Started

数据库“日志恢复”的核心机制：
- before-image：告诉你“修改前是什么样子”，用于撤销（UNDO）。
- after-image：告诉你“修改后是什么样子”，用于重做（REDO）。

### UNDO 和 REDO

| 类型 | 含义          | 场景 |
|:------:|:------------|:----------------|
| UNDO（撤销） | 撤销未提交事务的修改  | 回滚 / 崩溃恢复 |
| REDO（重做） | 重做已经提交的事务修改 | 崩溃恢复 |

- UNDO 用 before-image 覆盖
- REDO 用 after-image 覆盖

### ARIES 算法是什么？
ARIES（Algorithms for Recovery and Isolation Exploiting Semantics）
是 IBM 在 1992 年提出的一个标准数据库恢复算法。

#### 核心原则
1. Write-Ahead Logging (WAL)：写数据前，一定要先把日志写盘
   - 保证崩溃后能恢复
2. STEAL + NO-FORCE 策略
    - STEAL：允许未提交事务的脏页被写入磁盘（更灵活）
    - NO-FORCE：提交事务时不一定要立刻写页到磁盘（性能高）
    - 所以恢复必须能识别哪些该重做、哪些该撤销
3. 重复历史 + 撤销未提交事务
   - 重复历史：崩溃前的所有操作都要重做
   - 撤销未提交事务：撤销所有未提交事务的操作

#### ARIES 的三个阶段
1. Analysis（分析阶段）
   - 从检查点开始，分析日志，找出哪些事务是Loser（未提交的），哪些是Winner（已提交的）
2. Redo（重做阶段）
   - 从最早的脏页开始，重做所有已提交事务的操作
3. Undo（撤销阶段）
   - 撤销所有未提交事务的操作，按逆序撤销 

### 为什么工业数据库不用“页级锁 + 整页物理 UNDO”

- 页级锁粒度太大，影响并发

工业数据库采用：行级锁（row-level locking） 或 键值锁（index/key-level locking）

这意味着：
- 多个事务可以同时修改同一页的不同记录；
- 崩溃时，你不能直接用“整页 before-image 覆盖回去”；
- 因为那样可能把别的事务的修改也一起抹掉。

所以得用**逻辑UNDO**

### 逻辑UNDO是什么
- 物理UNDO = 把整页恢复成修改前的快照
- 逻辑UNDO = 根据日志描述“做了什么”，再反向执行“撤销这个操作”

## 2. Rollback - UNDO

rollback 在事务中止（abort）时、释放锁之前被调用。

rollback 核心工作流：
1. 找到该事务在日志文件里的起点
   - 每个事务在日志中从一条 BEGIN 记录开始
   - 系统保存了一个 tidToFirstLogRecord 映射，可以直接告诉你这个事务从日志文件的哪个位置开始
   - rollback 要从这里开始“回看”它做过什么
2. 正向扫描这个事务写过的日志记录（UPDATE 记录）
   - 这些 UPDATE 每条都带着两个镜像: before-image, after-image
3. 逆序撤销（UNDO）
   - 如果同一个页面被更新了多次，必须 **从后往前** 撤销。
   > 因为最后一次的 before-image 才对应“上一次修改前”的状态。
   - 所以 rollback 需要 反向遍历 所有 UPDATE 日志
4. 对于该事务的每一条 UPDATE 记录（从最后一条往前）：
   - 取出 before-image（旧版本的页面内容）
   - 把 before-image 直接写回对应的表文件（覆盖掉修改后的页面）
   - 通知 BufferPool：这个页面无效了，丢掉缓存中的版本（discardPage），防止脏页再被写回
5. 完成回滚
   - 当所有 UPDATE 都撤销完毕，数据库文件就恢复成事务开始前的状态
   - rollback 不负责写 ABORT 记录（调用它的上层会负责）
   - 也不释放锁——rollback 之后事务还没完全结束，锁由上层在 abort 逻辑里释放

### Exercise 1

Implement LogFile.rollback(TransactionId tid)

#### 为什么有两个synchronized？
- 先锁 BufferPool
- 再锁 LogFile

**防止死锁**
- 假设你把 rollback() 声明成 synchronized
- 那就是先锁 LogFile.this，然后它再去调用 Database.getBufferPool() 里的同步方法
- 而别的线程可能已经锁住了 BufferPool，然后在写日志时又想拿 LogFile 的锁
> rollback() 内部要访问 BufferPool（比如 discardPage()），而 BufferPool 本身也有很多 synchronized 方法（比如 flushPage()、getPage()）


但凡一个函数会：
> 同时操作日志和缓冲池（比如 rollback、recover、checkpoint）
就必须**手动写嵌套锁**。

#### 对于正向扫描的实现
需要知道simpledb里的type的不同格式，参加注释

## 3. Recovery - REDO + UNDO

1. 读 checkpoint（定位恢复起点），从这里开始 REDO
   - 日志文件开头（offset = 0）存着“最后一个 checkpoint 的偏移”
     - 没有 checkpoint（= -1）：startPos = 8（跳过文件头 8 字节）
     - 有 checkpoint：跳到该 checkpoint 记录，解析出当时活跃事务集合及它们各自的 firstOffset
       - 若活跃事务为空：startPos = checkpointOffset 
       - 若 非空：startPos = 所有活跃事务 firstOffset 的最小值（minFirstOffset）
2. 正向扫描（顺序从 startPos 到 EOF）：只收集，不落盘
   只做统计与缓存，为“先 UNDO 后 REDO”做准备。
   需要维护四个东西：
   - winners：最终观察到 COMMIT 的事务 id 集合；
   - losers：最终仍未 COMMIT/ABORT 的事务 id 集合（初始包含 checkpoint 活跃事务；扫到 BEGIN 也加入；扫到 COMMIT/ABORT 就移出）；
   - beforePagesByTid：每个事务 id → 按出现顺序记录 before-images 的列表；
   - afterPagesByTid：每个事务 id → 按出现顺序记录 after-images 的列表。
3. 执行恢复
   - UNDO losers
      - 对于每个 loser 事务，逆序遍历它的 before-images 列表（（确保多次更新同一页能回到该事务最早的状态））
      - 把每个 before-image 写回对应的表文件，恢复成修改前的状态
      - 通知 BufferPool 丢弃该页面的缓存版本（discardPage）
   - REDO winners
      - 对于每个 winner 事务，正序遍历它的 after-images 列表（REDO 要幂等，多次写结果一致）
      - 把每个 after-image 写回对应的表文件，重做该事务的修改
      - 通知 BufferPool 丢弃该页面的缓存版本（discardPage）

### Exercise 2

Implement LogFile.recover()

测试自始至终都有一个通过不了，没招了。。。

#### 关于 执行恢复 的顺序
ARIES 风格（真实数据库）

🔹 REDO → UNDO

重演所有历史，再撤销 loser。幂等、安全。即使崩溃时写到一半也能继续恢复。


