package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;

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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;  // Group By 的字段下标，告诉聚合器“按哪一列分组”
    private final Type gbfieldtype;  // Group By 的字段类型
    private final int afield;  // Aggregate 的字段下标，告诉聚合器“对哪一列进行聚合”
    private final Op what;  // 聚合操作

    // 用来存分组 -> 聚合中间状态，String 只能支持 COUNT，所以中间状态直接存 Integer
    private final Map<Field, Integer> groups = new HashMap<>();
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);

        // 只支持 COUNT 操作，直接把对应分组的计数加一
        groups.put(groupField, groups.getOrDefault(groupField, 0) + 1);

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
        // 3. 遍历 groups，构造每条结果的 Tuple，并加入 results 列表
        for (Map.Entry<Field, Integer> entry : groups.entrySet()) {
            Tuple tuple = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                // 没有分组，只有聚合结果
                tuple.setField(0, new IntField(entry.getValue()));
            } else {
                // 有分组，先设置分组字段，再设置聚合结果
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
            }
            results.add(tuple);
        }
        return new TupleIterator(td, results);
    }

}
