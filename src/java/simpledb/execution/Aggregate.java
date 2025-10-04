package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

import simpledb.common.Type;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;   // 输入数据来源
    private final int afield;   // Aggregate 的字段下标，告诉聚合器“对哪一列进行聚合”
    private final int gfield;   // Group By 的字段下标，告诉聚合器“按哪一列分组”；无分组时 = Aggregator.NO_GROUPING
    private final Aggregator.Op aop;

    private Aggregator aggregator;    // 具体的聚合器（Int 或 String）
    private OpIterator aggIter;       // 聚合结果的迭代器
    private TupleDesc outTd;          // 输出的 TupleDesc（列结构）

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (gfield == Aggregator.NO_GROUPING) {
            return null;
        }
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();

        // 1. 创建具体的Aggregator实例
        TupleDesc childTd = child.getTupleDesc();
        Type afieldType = childTd.getFieldType(afield);
        Type gfieldType = (gfield == Aggregator.NO_GROUPING) ? null : childTd.getFieldType(gfield);

        if (afieldType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(gfield, gfieldType, afield, aop);
        } else if (afieldType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gfieldType, afield, aop);
        } else {
            throw new IllegalArgumentException("Unsupported afield type: " + afieldType);
        }

        // 2. 遍历child，将所有tuple合并到aggregator中
        child.open();
        while (child.hasNext()) {
            Tuple tup = child.next();
            aggregator.mergeTupleIntoGroup(tup);
        }

        // 3. 获取aggregator的迭代器，并打开它
        aggIter = aggregator.iterator();
        aggIter.open();

        // 4. 构造输出的TupleDesc
        if (gfield == Aggregator.NO_GROUPING) {
            outTd = new TupleDesc(new Type[]{Type.INT_TYPE},
                    new String[]{aggregateFieldName()});
        } else {
            outTd = new TupleDesc(new Type[]{gfieldType, Type.INT_TYPE},
                    new String[]{groupFieldName(), aggregateFieldName()});
        }
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (aggIter != null && aggIter.hasNext()) {
            return aggIter.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (aggIter != null) {
            aggIter.rewind();
        }
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (outTd != null) {
            return outTd;
        } else {
            return computeOutTdFromChild();
        }
    }

    // 根据 child 的 TupleDesc（输入表的结构）计算本算子输出结果的 TupleDesc
    private TupleDesc computeOutTdFromChild() {
        // 获取数据输入的 TupleDesc
        TupleDesc childTd = child.getTupleDesc();

        // 获取GROUP_BY的类型
        Type gfieldType = (gfield == Aggregator.NO_GROUPING) ? null : childTd.getFieldType(gfield);

        // 根据是否分组构造输出结果的 TupleDesc
        if (gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(
                    new Type[]{Type.INT_TYPE},
                    new String[]{aggregateFieldName()}
            );
        } else {
            return new TupleDesc(
                    new Type[]{gfieldType, Type.INT_TYPE},
                    new String[]{groupFieldName(), aggregateFieldName()}
            );
        }
    }

    public void close() {
        // some code goes here
        super.close();            // ① 标记当前 Operator 已关闭，防止后续再被访问，再关注下游
        if (aggIter != null) aggIter.close();  // ② 关闭聚合结果迭代器
        child.close();            // ③ 最后关闭输入算子
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        // 替换数据来源；其余运行期对象在下次 open() 时重建
        this.child = children[0];   //  Aggregate（和 Filter 一样）只有一个输入
        this.aggregator = null;
        this.aggIter = null;
        this.outTd = null;
    }

}
