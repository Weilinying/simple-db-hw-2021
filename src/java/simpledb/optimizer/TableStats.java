package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();
        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            try {
                TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
                setTableStats(Database.getCatalog().getTableName(tableid), s);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    // 绑定这张表 & I/O 参数
    private final int tableId;
    private final int ioCostPerPage;

    // 访问表结构与数据的入口（为什么要 DbFile：能拿到 TupleDesc / 迭代所有 Tuple / 页数）
    private final DbFile file;
    private final TupleDesc td;
    private final int numPages; // 扫描代价 = numPages × ioCostPerPage

    // 统计数据
    private int totalTuples; // 构造时算出来

    // 每列的直方图（按下标存）
    private final Map<Integer, IntHistogram> intHists = new HashMap<>();
    private final Map<Integer, StringHistogram> stringHists = new HashMap<>();

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.td = file.getTupleDesc();
        this.numPages = ((HeapFile) this.file).numPages();
        this.totalTuples = 0;

        // 1. 先遍历一遍，拿到每个 int 列的 min/max
        int[] mins = new int[td.numFields()];
        int[] maxs = new int[td.numFields()];
        boolean[] seen = new boolean[td.numFields()]; // seen[i]：是否已经初始化过第 i 列的 min/max

        // 创建一个事务 ID，表扫描需要事务上下文
        TransactionId tid = new TransactionId();

        // 获取这张表的迭代器（DbFileIterator 可以顺序遍历所有 Tuple）
        DbFileIterator it = file.iterator(tid);

        try {
            // 确保先打开迭代器
            it.open();

            // 第一次遍历：统计各 INT 列的最小值/最大值、总行数
            while (it.hasNext()) {
                Tuple t = it.next();
                this.totalTuples++;

                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int v = ((IntField)t.getField(i)).getValue();
                        if (!seen[i]) {
                            mins[i] = v;
                            maxs[i] = v;
                            seen[i] = true;
                        } else {
                            if (v < mins[i]) mins[i] = v;
                            if (v > maxs[i]) maxs[i] = v;
                        }
                    }
                }
            }

            // 2. 根据 min/max 初始化直方图
            for (int i = 0; i < td.numFields(); i++) {
                if (td.getFieldType(i) == Type.INT_TYPE) {
                    if (!seen[i]) {
                        // 若某列没有出现过（极端情形：空表），给个退化直方图
                        intHists.put(i, new IntHistogram(NUM_HIST_BINS, 0, 0));
                    } else {
                        intHists.put(i, new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]));
                    }
                } else {
                    stringHists.put(i, new StringHistogram(NUM_HIST_BINS));
                }
            }

            // 3. 回到起点再遍历一次，将值灌入直方图
            it.rewind();
            while (it.hasNext()) {
                Tuple t = it.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        IntHistogram hist = intHists.get(i);
                        if (hist != null) {
                            int v = ((IntField)t.getField(i)).getValue();
                            hist.addValue(v);
                        }
                    } else {
                        StringHistogram hist = stringHists.get(i);
                        if (hist != null) {
                            String v = ((StringField)t.getField(i)).getValue();
                            hist.addValue(v);
                        }
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            throw new RuntimeException(e);
        } finally {
            try { it.close(); } catch (Exception ignored) {}
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ioCostPerPage * numPages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 返回应用该过滤后预计还剩多少行
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        // 边界1：选择率 <= 0，说明条件把所有行都过滤掉了，返回 0
        if (selectivityFactor <= 0) return 0;

        // 边界2：表本身没有行（或未知），无论选择率是多少，结果都是 0
        if (totalTuples <= 0) return 0;

        // 核心：总行数 × 选择率 = 期望保留的行数
        // 用 Math.round 做“四舍五入”，比直接截断更稳（例如 0.6 -> 1）
        int est = (int) Math.round(totalTuples * selectivityFactor);

        // 兜底保护：理论上 est 不会为负，这里取个 max(0, est) 保守返回非负数
        // （在本函数前两行的保护下，这一行通常不会起作用，仅作安全网）
        return Math.max(0, est);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * avgSelectivity 是在你还不知道具体常量（比如 WHERE x.city = ?）时，给优化器一个“平均情况下这个条件能筛掉多少行”的估计，这样优化器才能先算体量、再选更便宜的执行计划
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        if (field < 0 || field >= td.numFields()) return 1.0;

        // 从直方图中估计平均选择率
        if (td.getFieldType(field) == Type.INT_TYPE) {
            IntHistogram hist = intHists.get(field);
            if (hist != null) {
                // 直接调用 IntHistogram 的方法
                return hist.avgSelectivity();
            }
        } else {
            StringHistogram hist = stringHists.get(field);
            if (hist != null) {
                // 直接调用 StringHistogram 的 方法
                return hist.avgSelectivity();
            }
        }
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     *            TupleDesc 里该列的位置
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     *            比较用的常量值
     * estimateSelectivity(2, Predicate.Op.GREATER_THAN, new IntField(30)) -> 估计“第三列 > 30”的选择率
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (field < 0 || field >= td.numFields()) return 1.0;
        if (constant == null) return 1.0;

        // 从直方图中估计选择率
        if (td.getFieldType(field) == Type.INT_TYPE && constant.getType() == Type.INT_TYPE) {
            IntHistogram hist = intHists.get(field);
            if (hist != null) {
                int v = ((IntField)constant).getValue();
                // 直接调用 IntHistogram 的方法
                return hist.estimateSelectivity(op, v);
            }
        } else if (td.getFieldType(field) == Type.STRING_TYPE && constant.getType() == Type.STRING_TYPE) {
            StringHistogram hist = stringHists.get(field);
            if (hist != null) {
                String v = ((StringField)constant).getValue();
                // 直接调用 StringHistogram 的方法
                return hist.estimateSelectivity(op, v);
            }
        }
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return totalTuples;
    }

}
