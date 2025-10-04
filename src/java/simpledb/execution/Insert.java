package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;

    // 控制只执行一次插入并只产出一次结果
    private boolean done = false;
    // 输出：单列 INT
    private final TupleDesc outTd = new TupleDesc(new Type[]{Type.INT_TYPE});

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;

        // 校验 child 的 TupleDesc 与目标表一致（否则抛错）
        TupleDesc childTd = child.getTupleDesc();
        TupleDesc targetTd = Database.getCatalog().getTupleDesc(tableId);
        if (!childTd.equals(targetTd)) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return outTd;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        // 只重置 child 的游标，不重置 done，
        // 防止 rewind 后再次执行插入导致重复写入
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 第一次调用：把 child 的所有 tuple 插入 tableId，返回 1 行 1 列（INT）的结果，值为插入条数
     * 第二次调用：返回 null
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int count = 0;

        if(done) return null;

        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
                count++;
            } catch (IOException e) {
                throw new DbException("Insert failed.");
            }
        }
        done = true;
        Tuple result = new Tuple(outTd);
        result.setField(0, new IntField(count));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if(children == null || children.length == 0 || children[0] == null) {
            throw new IllegalArgumentException("Insert requires exactly one non-null child");
        }
        this.child = children[0];
    }
}
