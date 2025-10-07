# Lab3: Query Optimization

## 0. Introduction

### 什么是Query Optimization？

- 查询优化器就是数据库的大脑
- 你写了一条 SQL，它不会直接照字面意思执行，而是会生成多种可能的执行计划（plan），然后估算每种计划的代价（cost），再选出代价最小的那个去执行
- “查询优化器” ≈ “JOIN 顺序选择器”
- 最优 JOIN 顺序就是性能的核心决定因素

### 目标

1. 实现Selectivity Estimation（选择性估计）
   - 估算过滤和连接条件会筛掉多少行，以及扫描代价是多少
2. 实现基于代价的查询优化器（Cost-Based Query Optimizer）
    - 利用这些估计，计算不同查询计划的代价，并用 Selinger 动态规划算法找到最优 JOIN 顺序

### 流程图

```mermaid
SQL 查询
   ↓
解析器 (Parser)      ← 把 SQL 转换为抽象语法树
   ↓
逻辑计划 (Logical Plan)  ← JOIN、FILTER、SCAN 等算子
   ↓
查询优化器 (Optimizer)  ← ✅ 你要实现的部分
      ↳ 估算选择性 (Selectivity Estimation)
      ↳ 枚举执行计划
      ↳ 计算代价 (Cost Estimation)
      ↳ 选择最优计划
   ↓
物理计划 (Physical Plan)
   ↓
执行器 (Executor)
   ↓
结果输出
```

## 1. Statistics Estimation

### Exercise 1: IntHistogram.java

**StringHistogram的使用情况**
```sql
SELECT * FROM Students WHERE name = 'Alice';
SELECT * FROM Students WHERE city = 'Melbourne';
SELECT * FROM Orders WHERE status = 'pending';
```

StringHistogram 可以复用 IntHistogram 来估计字符串列的选择率。

核心：必须把字符串映射成“保持字典序的整数”（单调映射）：s1 < s2 ⇒ f(s1) < f(s2)

### Exercise 2: TableStats.java

#### 目标
为每张表建统计信息（直方图 + 行/页数），并基于此给优化器提供选择性与代价估计。
- **构造函数**：遍历表的所有行，统计每列的 min/max，然后创建 IntHistogram/StringHistogram，并再次遍历表填充直方图

#### 为什么要加transaction

1. **Transaction是什么**
   
   transaction 可以被理解为通行证，每一个访问数据库的操作（读 / 写 / 扫描 / 插入 / 删除）都必须知道**是谁(TransactionId)**在干这件事，数据库才能判断：
   - 要不要给它加锁？
   - 是不是应该阻塞（比如别的事务在写）？
   - 出错时能不能回滚？
   - 执行完要不要释放资源？

2. **什么情况下需要Transaction**

   只要你涉及到底层存储访问（pages / buffer pool），就必须带着事务 ID，因为这一层才真正关心“是谁来访问数据”。

### Exercise 3: Join Cost Estimation

#### 目标
1. JOIN 的代价是多少(join cost) -> 直接决定查询计划的代价
2. JOIN 结果大概有多少行？（join cardinality）-> 决定下一步算子（比如再 join、再 filter）时的代价计算

#### 实现 estimateJoinCost
joinCost = cost1 + card1 * cost2 + card1 * card2

- cost1：扫描外层表的成本
- card1 * cost2：内层表会被扫描 card1 次
- card1 * card2：每个组合都要比较一次（CPU 成本）

#### 实现 estimateJoinCardinality
三种常见情况:
1. 等值连接(t1.a = t2.a) + 一边是主键（primary key）
   例如：
   ```sql
    SELECT * FROM Students s JOIN Courses c ON s.course_id = c.id;
   ```
   由于主键一侧每行最多匹配一行，所以
   joinCard = card1 (假设 t1 是非主键表)
2. 等值连接 + 双方都是主键
   joinCard = min(card1, card2)
   JOIN 后的总行数不会超过较小表的行数，因为主键一侧每行最多匹配一行。
3. 等值连接 + 双方都不是主键（最难估计）
   - 如果两张表所有值都一样 → 结果可能接近 card1 * card2（笛卡尔积）
   - 如果完全不重叠 → 结果可能是 0
3. 范围连接（<, >, BETWEEN 等）
   这种没办法精确估计，lab建议直接假设是“交叉乘积的一个固定比例”

## 2. Join Ordering

目标：实现 Selinger 优化器（经典的基于动态规划的连接顺序优化算法）
- 因为不同连接顺序的代价差异可能非常大，数据库优化器要决定最优的连接顺序

### Selinger优化器
实现思路：用 动态规划（DP）按子集做最优子结构
- 某个表子集 s 的最优计划 = 在它的一个**更小的最优子计划** s'上，把剩下一张表接上去的最便宜方式。

伪代码：
```text
1. j = 所有 join 节点的集合
2. 对于 i 从 1 到 |j|：                    ← 子集大小（从 1 个节点到所有节点）
3.     对于 s ∈ { 所有大小为 i 的 join 节点子集 }：
4.         bestPlan = {}
5.         对于 s' ∈ { s 的所有大小为 i-1 的子集 }：
6.              subplan = optjoin(s')         ← 最优的子计划（之前已经计算过，前一次扫描的结果存表里了，然后这次就可以直接用）
7.              plan = 将 (s - s') 加入 subplan 的最佳方式
8.              如果 cost(plan) < cost(bestPlan)：
9.                  bestPlan = plan
10.        optjoin(s) = bestPlan              ← 存储这个子集的最优计划
11. 返回 optjoin(j)                         ← 返回整个连接集合的最优计划
```
### Exercise 4: Join Ordering

实现思路：
1.	创建一个 PlanCache 来保存每个「子集」的最优计划。
2.	遍历子集大小 i 从 1 到 joins.size()：
   - 枚举所有大小为 i 的子集 s（用 enumerateSubsets()）
   - 对这个子集，尝试把每个 join 拿出来当作「最后一个 join」，并把它和「剩下的子集」最优解拼接
   - 调用 computeCostAndCardOfSubplan() 得到代价和输出行数
   - 记录最优的那一个 plan 到 PlanCache
3.	最终，PlanCache 中记录了所有子集的最优 plan，取完整集合的 plan 返回即可。


