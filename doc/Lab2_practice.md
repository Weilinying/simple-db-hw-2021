# Lab 2: SimpleDB Operators

实现：
- 编写一组运算符，以实现表修改（例如，插入和删除记录）、选择、连接和聚合 -> 实现一个可以对多个表执行简单查询的数据库系统
- 处理Lab1中的BufferPool的问题：当数据库在运行期间访问的页面数量超过内存所能容纳的最大页数时，会出现问题
    - 设计一个**淘汰策略(eviction policy)**，当内存满时，选择一个页面进行替换

## 0. 为什么数据库内部几乎所有算子都用 Iterator（OpIterator）

- **统一接口**：Iterator 提供了一种统一的方式来遍历数据，无论数据存储在内存中还是磁盘上，或者是通过网络获取的。这样，数据库系统的各个组件可以通过相同的接口进行交互，而不需要关心底层数据的具体存储方式。
- **延迟加载**：Iterator 允许按需加载数据，而不是一次性将所有数据加载到内存中。这对于处理大规模数据集尤为重要，因为它可以节省内存并提高性能。
- **组合操作**：数据库查询通常涉及多个操作（如过滤、连接、聚合等）。使用 Iterator，可以轻松地将这些操作组合在一起，形成一个执行树，每个操作都可以独立处理数据流。
- **结果条数不确定**：在数据库查询中，结果集的大小通常是未知的。Iterator 允许逐条获取结果，而不需要预先知道结果的总数，这对于处理动态数据集非常有用。
- **简化资源管理**：Iterator 通常包含打开、关闭和重置等

## 1. Filter and Join
### Exercise 1
***  

* src/java/simpledb/execution/Predicate.java
* src/java/simpledb/execution/JoinPredicate.java
* src/java/simpledb/execution/Filter.java
* src/java/simpledb/execution/Join.java

***  

#### 实现 Predicate.java
1. **Predicate是啥？**

    Predicate（谓词） 在数据库里就是一个“条件表达式”——它会对每一行（tuple）判断 是否满足某个条件（true/false） 
    比如 age > 18 就是一个 predicate。

2. **为什么是implements Serializable？**
    - 什么是 Serializable？
        - Serializable 是 Java 提供的一个接口，作用是告诉JVM，一个类的对象可以被序列化（转换为字节流）和反序列化（从字节流恢复为对象）。
        - 序列化就是把对象的内存状态（比如字段值）转成二进制流，以便：
          - 写入磁盘（例如保存查询计划到文件、日志、快照）；
          - 通过网络传输（例如分布式数据库节点之间传递谓词，e.g. 发到另一台服务器，让它也执行相同的查询）；
          - 缓存/恢复对象（比如在事务恢复时重建查询条件）。
        - 简单来说：**序列化 = 把内存里的对象“拍扁”成一串字节，方便存储或传输；反序列化 = 再把这串字节“还原”成对象**
    - 为什么 Predicate 要实现 Serializable？
        - 数据库在很多场景下需要把这个条件 (Predicate) “持久化”或“传递”出去


| 场景 | 为什么需要序列化 |
|------|------------------|
| 📁 写入日志 | 在事务日志中可能需要记录当前的查询条件（例如 `age > 20`），以便系统崩溃后恢复时能重建查询上下文。 |
| 📤 分布式执行 | 在分布式数据库中，`Predicate` 可能需要发送到其他节点，让它们在本地执行相同的过滤逻辑。 |
| 🧠 缓存计划 | 查询优化器生成的执行计划（其中包含 `Predicate`）可能会被序列化后缓存，下次相同查询可以直接复用，提高效率。 |
| 💾 调试 / 测试 | 在调试或测试中，有时需要把对象序列化保存下来，然后再加载重放，以便重现问题或验证行为。 |

3. **serialVersionUID的作用是？**

   用来校验「序列化对象」和「反序列化类」是否匹配。

4. **filter()方法的作用？**

    1. 从 tuple 中取出第 field 个字段
   2. 根据 op（操作符）和 operand（值）进行比较
   3. 返回比较结果（true/false）

#### 实现 Filter.java

1. **什么是Filter?**

    从输入的元组（tuples）流中筛选出满足特定条件的那些，就像 SQL 里的 WHERE 子句。

2. **Filter的两个参数**

    - Predicate p：过滤条件（要筛选的标准，例如 age > 30）
      - 调用 predicate.filter(tuple)，返回 true 就说明这行数据通过筛选
    - OpIterator child：数据来源
      - Filter 通过 child.next() 不断获取数据行，然后用 p.filter(tuple) 判断是否符合条件/要不要留下。

3. **为什么rewind()没有super**

   open() 和 close() 是所有算子都“必须”支持的生命周期操作；父类没有实现rewind是因为rewind是可选功能，交由子类自己决定。

#### 实现 Join.java
1. **child1.getTupleDesc().getFieldName(predicate.getField1())的解释**

    child1，child2是表名（两个数据来源）

    field1是第一个表参与 join 的字段索引，field2是第二个表参与 join 的字段索引。

2. **实现fetchNext()**

    - 对于Join的两个输入算子 child1 和 child2，通常叫child1左表，child2右表。

    - 嵌套循环连接（Nested Loops Join）的逻辑是这样的：

    ```text
    for 左表 (child1) 中的每个 tuple:
        重置右表 (child2) 到开头
        for 右表 (child2) 中的每个 tuple:
            如果满足 predicate 条件 -> 实现 JOIN
    ```
    - fetchNext() 每次只吐一条结果，然后就返回。所以下次再被调用时，要从上次的扫描状态继续。这就需要把“外层循环当前用的左表 tuple”记在成员变量 leftTuple 上。
    - 所以在代码实现里不能使用for each，而是得用while循环 + 成员变量来保存状态。

## 2. Aggregate

**Aggregate是啥**
- 在 SQL 中，聚合操作就是对一组数据进行“汇总计算”，最常见的 5 个操作就是 `COUNT`、`SUM`、`AVG`、`MIN` 和 `MAX`。
- 聚合通常和 `GROUP BY` 一起使用，可以对数据进行分组后再计算每组的汇总值。
- 比如，`SELECT department, COUNT(*) FROM employees GROUP BY department;` 这个查询会按部门分组，然后计算每个部门的员工数量。
- GROUP BY 决定“分几组”、“怎么分组”，而聚合函数决定“每组里面怎么计算”。

**关于 Aggregate 这个功能的实现**
- 在构造 Aggregator 时，你告诉它要用哪种操作（COUNT, SUM, AVG 等）和是否有分组字段
- 聚合计算是通过 Aggregator 接口实现的，调用 mergeTupleIntoGroup() 把每条 tuple 合并进来。所有数据处理完后（所以要把中间状态存起来），iterator() 返回聚合结果：
  - 有分组时是 (groupValue, aggregateValue)
  - 无分组时是 (aggregateValue)

### Exercise 2
***  

* src/java/simpledb/execution/IntegerAggregator.java
* src/java/simpledb/execution/StringAggregator.java
* src/java/simpledb/execution/Aggregate.java

*** 

#### 实现IntegerAggregator.java
1. **IntegerAggregator的成员变量解释**

   - gbfield：分组字段索引
   - gbfieldtype：分组字段类型
   - afield：聚合字段索引
   - what：聚合操作类型（COUNT, SUM, AVG, MIN, MAX）

2. **mergeTupleIntoGroup()的实现思路**

   **Step1**: merge的目标是 `按照 group-by 字段分类` 和 `每个组保存当前的中间聚合状态`。设计一个哈希表保存“分组 → 中间状态”

      - 使用 Field：分组的 key（如果无分组，就用 null 作为 key）；AggState：存储该组的中间状态。
      ```
      Map<Field, AggState> groups = new HashMap<>();
      ```

   **Step2**: 怎么维护中间状态 → 设计 AggState -> 1. 状态是static，因为是内部辅助结构，不需要依赖外部实例 2. 需要存什么

   - count：记录该组的记录数（COUNT）
   - sum：记录该组的总和（SUM）
   - min：记录该组的最小值（MIN）
   - max：记录该组的最大值（MAX）
   - avg：可以通过 sum 和 count 计算得到（AVG）
   
   另外提供 add(int v)、result(Op op) 两个方法，便于更新 & 出结果

   **Step3**: 实现 mergeTupleIntoGroup()

   1. 从 tuple 里取出 group-by 字段和聚合字段的值
   2. 根据 group-by 字段值找到对应的 AggState（如果没有就创建一个新的）
   3. 调用 AggState 的 add() 方法更新中间状态
   4. 把更新后的 AggState 放回Map

3. **为什么state.add(aggValue)实现了把更新后的 AggState 放回Map**

   因为 Java 的 Map 存储的是对象的引用，state 是从 Map 中取出的 AggState 对象的引用，调用 state.add(aggValue) 实际上是修改了这个对象的内部状态，而不是替换了 Map 中的引用。

4. **iterator()的实现思路**

   **Step1**: iterator()的目标是 `把前面用 mergeTupleIntoGroup() 累积下来的聚合结果，以迭代器的形式返回出来`

   **Step2**: 数据结构应该长啥样 -> 把 Map 转换成 Tuple 列表

      - 对于创建的tuple，有 group 的话是 (groupVal, aggVal)，否则只有 aggVal

   **Step3**: 返回一个迭代器 -> TupleIterator (把 List<Tuple> 包装成一个 OpIterator)

#### 实现StringAggregator.java

String只支持 COUNT 聚合操作，其他和 IntegerAggregator 类似。

#### 实现Aggregate.java

1. **为什么Aggregate.open()这么复杂?**

   - 对于大多数算子（比如 Filter、SeqScan）来说，open() 的任务很简单，打开自己，打开下游（child）数据源，之后就可以在 fetchNext() 里一条一条处理数据了。
   - 但是 Aggregate 不一样，因为它需要先把所有数据都读完，计算出聚合结果，然后才能开始返回结果。
   - 所以Aggregate.open()需要做4件事
      - 调用 super.open() 打开自己
      - 创建正确的 Aggregator 实例 （IntegerAggregator or StringAggregator）
      - 遍历 child，把所有数据都合并进 Aggregator
      - 从聚合器拿到结果迭代器
      - 构造输出 TupleDesc （因为输出和输入结构不一样）

## 3. HeapFile Mutability

**什么是 HeapFile?**
HeapFile 就是用来在磁盘上存储表数据的最基本单位文件，它：
- 无序地存储表中的所有数据行（tuples）；
- 按照「页 (Page)」为基本单位读写磁盘；
- 每一页里装多个元组（tuple）；
- 页之间没有顺序要求，也没有索引结构。

### Exercise 3
***  

* src/java/simpledb/storage/HeapPage.java
* src/java/simpledb/storage/HeapFile.java<br>
  (Note that you do not necessarily need to implement writePage at this point).

***

#### 实现HeapPage.java

1. **deleteTuple()的实现思路**

    - Step1: 检查 tuple 是否属于该页
    - Step2: 校验该 slot 当前确实是占用状态
    - Step3: 清空 tuples[i] 并更新 header 位

2. **insertTuple()的实现思路**

    - Step1: 校验 TupleDesc 与本页一致
    - Step2: 找到第一个空闲的 slot
    - Step3: 把 tuple 放进去，设置 RecordId，并更新 header 位

#### 实现HeapFile.java

实现的是 HeapFile 这一层的“表级别”增删，而不是页内具体的位图/槽位操作（那是 HeapPage 的活儿）

1. **insertTuple()的实现思路**

    - Step1: 遍历所有页，找到一个有空闲槽位的页
    - Step2: 如果找到了，就调用该页的 insertTuple() 方法插入
    - Step3: 如果没找到，就创建一个新页，插入到新页

2. **为什么insertTuple() 不直接 new 一个空 HeapPage 对象然后加进去**

   HeapFile 就是磁盘上真正存放表数据的 .dat 文件，数据库不是直接把页存在内存结构里，而是：

   - 页（Page） 是磁盘文件中的一段固定长度的数据（通常 4096 字节）
   - HeapFile 是这些页在磁盘上顺序排列的集合
   - HeapPage 是内存中对这段字节的“解释”

   因此，HeapFile 的页必须“先写到磁盘上”才能被 BufferPool 读取、管理、加锁。

2. **deleteTuple()的实现思路**

    - Step1: 获取 tuple 所在的页
    - Step2: 调用该页的 deleteTuple() 方法删除

#### 实现BufferPool.java

BufferPool.insertTuple(...) / deleteTuple(...) 是**最高层（系统级/缓存与事务协调层）的入口方法**。

要做的事：

1. 找到这条记录所属的 DbFile（通常是 HeapFile）；
2. 转发调用到 HeapFile.insertTuple(...) / deleteTuple(...)；
3. 接住 HeapFile 返回的被修改的页列表，把这些页：
   - 标记为脏页（dirty），关联上当前事务 ID；
   - 放入/更新 BufferPool 的缓存中（如果不在缓存里）。
   
## 4. Insertion and deletion

**为什么insert和delete需要一个boolean done，别的operator不需要?**
- insert和delete是有副作用（side effect）的operator，它们会修改数据库的状态（插入或删除记录），而其他operator（如select, join, aggregate）只是读取数据，不会改变数据库的内容。
- 如果 fetchNext() 被上层调用两次，而你没有 done 控制，就会插入两遍，数据翻倍.
- 
### Exercise 4
***  

* src/java/simpledb/execution/Insert.java
* src/java/simpledb/execution/Delete.java

***  

#### 实现Insert.java

1. **为什么insert的实现逻辑在fetchNext()里?**

   - Insert 是一个算子（Operator），它的职责是“从 child 读取数据，然后插入到目标表中”。
   - 所有算子（Operator）都遵循一个非常典型的执行模型，叫做 Volcano 模型（也叫 pull 模型），流程如下：
      ```
     open()       // 初始化、准备资源
      fetchNext()  // “拉取”一条结果 tuple
      fetchNext()  // 再拉一条
      ...
      close()      // 释放资源
     ```
   - 每次 fetchNext() 调用都相当于对算子说：“给我下一条结果”
   - 所以在数据库执行管道里，所有“真正干活”的逻辑都应该放在 fetchNext() 里，不管是过滤、连接、投影还是插入

2. **为什么不能放在open()里?**

   - open() 只负责初始化和准备资源，它应该是无副作用的，不应该执行实际的插入操作。
   - 如果把插入逻辑放在 open() 里，那么每次打开算子时都会执行插入，这不符合 Volcano 模型的设计原则 （数据库执行模型要求：“所有会产生结果（或者副作用）的操作都必须出现在 fetchNext() 中。”）。
   - 另外，open() 只会被调用一次，而 fetchNext() 可以被调用多次，这样可以更灵活地控制插入操作。
   - 执行计划可能被多次 rewind 

3. **insert的getTupleDesc()为什么返回不是原始数据结构?**

   - Insert 算子的输出是“插入了多少条记录”，而不是原始数据行本身。
   - 因此，Insert 的输出结构（TupleDesc）应该是一个单字段的整数类型，表示插入的记录数。
   
## 5. Page eviction

### Exercise 5
***  

* src/java/simpledb/storage/BufferPool.java

***

#### 实现BufferPool.java

1. **理解BufferPool的职责**

   | 职责 | 对应功能 | 代码体现 |
   |------|----------|----------|
   | 📥 负责读写缓存页 | 把磁盘页加载到内存，缓存起来供查询使用 | `getPage()` |
   | 🧹 负责管理缓存容量 | 达到上限就驱逐旧页，保证内存不爆 | `evictPage()` |
   | 💾 负责脏页写回磁盘 | 把修改过的页刷回磁盘，保证数据持久性 | `flushPage()` |

2. **Page eviction的实现思路**

   采用STEAL策略：磁盘上永远只包含已提交事务的数据。

   - Step1: 选择一个页面进行替换
      - 这里用的是 LRU（最近最少使用）策略
      - 维护一个 List<PageId> lruList，记录页的使用顺序
      - 每次访问一个页，就把它移到 lruList 的末尾
      - 要驱逐时，就选 lruList 的头部（最久没用的页）
   - Step2: 如果被驱逐的页是脏页，就抛 DbException，不驱逐、不 flush
   - Step3: 从缓存中移除这个页

3. **LRU是什么**

   - LRU（Least Recently Used，最近最少使用）是一种常见的缓存替换策略，核心思想是“如果一个数据最近被访问过，那么它在未来一段时间内也很可能会被再次访问”。
   - LRU 的实现思想：
      - LRU 通常维护一个“访问顺序列表”
      - 每次访问（getPage 或 putPage）都把页移动到列表尾部（表示“最近刚用过”）
      - 当缓存满了需要驱逐页时，就移除列表头部的页（表示“最久没用”）

4. **Page eviction要注意什么**

   优先驱逐干净页（不是脏页），因为脏页需要写回磁盘，开销更大。

## 6. Query Walkthrough 

从底层角度看 SQL 是怎么一步步执行的

```java
package simpledb;

import java.io.*;

/**
 * 这个程序演示了在 SimpleDB 中如何用“算子组合”的方式手动执行一条 SQL 查询：
 *
 * 等价于：
 * SELECT *
 * FROM some_data_file1, some_data_file2
 * WHERE some_data_file1.field1 = some_data_file2.field1
 *   AND some_data_file1.field0 > 1;
 *
 * 程序步骤：
 * 1. 定义表结构（schema）
 * 2. 创建表对象并注册到系统目录（Catalog）
 * 3. 构造查询计划（SeqScan → Filter → Join）
 * 4. 执行查询并输出结果
 */
public class jointest {

    public static void main(String[] argv) {
        // ① 定义一个三列的表结构（TupleDesc：描述一行数据的“模式”）
        // 这里的每一列都是 INT 类型，字段名为 field0, field1, field2
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};
        TupleDesc td = new TupleDesc(types, names);

        // ② 创建两个表对象（HeapFile），并注册到数据库的 Catalog 中
        // 注意：这里的 .dat 文件是已存在的二进制数据文件，存储了表的数据页
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1, "t1"); // 注册为表名 t1

        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
        Database.getCatalog().addTable(table2, "t2"); // 注册为表名 t2

        // ③ 创建一个事务 ID（在 SimpleDB 中每次查询都在事务上下文中执行）
        TransactionId tid = new TransactionId();

        // ④ 构造两个顺序扫描算子（SeqScan）来遍历两个表的数据
        // SeqScan 是最底层的算子，负责从磁盘一页页读出元组
        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // ⑤ 创建一个过滤算子（Filter）用于实现 WHERE 子句中的 “t1.field0 > 1”
        // Predicate(0, Op.GREATER_THAN, new IntField(1)) 表示第0列 > 1
        // Filter 会在 SeqScan 读到每个元组后，对其进行条件判断，过滤掉不满足的元组
        Filter sf1 = new Filter(
                new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1)),
                ss1 // 上游数据来源：t1 的顺序扫描
        );

        // ⑥ 创建一个连接条件（JoinPredicate）：t1.field1 == t2.field1
        // 参数含义：左表第1列 == 右表第1列
        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);

        // ⑦ 创建 Join 算子，将过滤后的 t1 与 t2 连接起来
        // Join 算子会从左子算子 (sf1) 和右子算子 (ss2) 中不断取元组，并进行连接操作
        Join j = new Join(p, sf1, ss2);

        // ⑧ 执行查询：打开算子，遍历连接结果，并打印输出
        try {
            j.open(); // 打开算子链（从下到上依次 open）
            while (j.hasNext()) { // 不断检查是否还有下一条结果
                Tuple tup = j.next(); // 获取下一条连接结果元组
                System.out.println(tup); // 打印输出
            }
            j.close(); // 查询结束后关闭算子
            Database.getBufferPool().transactionComplete(tid); // 提交事务

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```


