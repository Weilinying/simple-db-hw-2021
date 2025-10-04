package simpledb.execution;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;  // Group By 的字段下标，告诉聚合器“按哪一列分组”
    private final Type gbfieldtype;  // Group By 的字段类型
    private final int afield;  // Aggregate 的字段下标，告诉聚合器“对哪一列进行聚合”
    private final Op what;  // 聚合操作

    // 用来存分组 -> 聚合中间状态
    private final Map<Field, AggState> groups = new HashMap<>();

    // 中间状态（支持 SUM/AVG/MIN/MAX/COUNT）
    private static class AggState {
        long sum = 0;
        int count = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        void add(int v) {
            sum += v;
            count += 1;
            if (v < min) min = v;
            if (v > max) max = v;
        }

        int result(Op op) {
            switch (op) {
                case COUNT: return count;
                case SUM:   return (int) sum;
                case AVG:   return count == 0 ? 0 : (int) (sum / count);
                case MIN:   return count == 0 ? 0 : min;
                case MAX:   return count == 0 ? 0 : max;
                default: throw new IllegalArgumentException("Unsupported op: " + op);
            }
        }
    }

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //  1. 计算分组键：有分组用分组列的 Field；无分组用 null
        Field groupKey = gbfield == NO_GROUPING ? null : tup.getField(gbfield);

        // 2. 取出聚合字段的值
        IntField aggField = (IntField) tup.getField(afield);
        int aggValue = aggField.getValue();

        // 3. 更新中间状态
        groups.putIfAbsent(groupKey, new AggState());
        AggState state = groups.get(groupKey);

        // 4. 根据聚合操作更新状态
        state.add(aggValue);

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        // 1. 创建一个空的列表，用来存储最终的聚合结果（每条结果是一个 Tuple）
        List<Tuple> results = new ArrayList<>();

        // 2. 构造 results 的 TupleDesc
        TupleDesc td;
        // 如果没有分组，输出只有一列（聚合结果）
        // 如果有分组，输出有两列（分组字段 + 聚合结果）
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        // 3. 遍历所有分组，计算每个分组的最终聚合结果
        for (Map.Entry<Field, AggState> entry : groups.entrySet()) {
            Field groupKey = entry.getKey();
            AggState state = entry.getValue();

            // 创建一个新tuple来存储结果
            Tuple tuple = new Tuple(td);

            // 写聚合结果
            if (gbfield == NO_GROUPING) { // 没有分组 → 输出格式为 (aggregateValue)
                tuple.setField(0, new IntField(state.result(what)));
            } else { // 有分组 → 输出格式为 (groupValue, aggregateValue)
                tuple.setField(0, groupKey);
                tuple.setField(1, new IntField(state.result(what)));
            }

            // 4. 把这一条聚合结果加入结果列表
            results.add(tuple);
        }

        // 5. 返回结果的迭代器
        return new TupleIterator(td, results);
    }

}
