package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int buckets;
    private final int min;
    private final int max;
    private final double width;    // 每个桶覆盖的范围宽度
    private final int[] bucketCounts; // 每个桶中的值的数量
    private int totalCount; // 所有值的总数（元组总数）

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.width = (double)(max - min + 1) / buckets;
        this.bucketCounts = new int[buckets];
        this.totalCount = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v < min || v > max) {
            throw new IllegalArgumentException("Value out of range");
        }

        // 找到这个value属于哪个bucket
        int bucketIndex = (int)((v - min) / width);

        // 处理边界情况
        if (bucketIndex >= buckets) {
            bucketIndex = buckets - 1; // edge case: v == max
        }

        bucketCounts[bucketIndex]++;
        totalCount++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value 落在 [0,1] 的概率
     * 1.0 表示“几乎所有元组都满足这个谓词”
     * 0.0 表示“几乎没有元组满足这个谓词”
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        if (totalCount == 0) {
            return 0.0; // Avoid division by zero
        }

        // 如果v在范围之外，直接返回结果
        if (v < min) {
            switch (op){
                case NOT_EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    return 1.0;
                case EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return 0.0;
                default:
                    return -1.0; // Unknown operator
            }
        }
        if (v > max) {
            switch (op){
                case NOT_EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return 1.0;
                case EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    return 0.0;
                default:
                    return -1.0; // Unknown operator
            }
        }

        // 计算selectivity需要的信息
        int bucketIndex = (int)((v - min) / width);

        if (bucketIndex >= buckets) {
            bucketIndex = buckets - 1; // edge case: v == max
        }

        double b_left = min + bucketIndex * width; // bucket左边界
        double b_right = b_left + width; // bucket右边界
        double b_height = bucketCounts[bucketIndex]; // bucket高度（频数）
        double b_freq = b_height / totalCount; // bucket频率（频数/总数）

        switch (op){
            // P (X = v))
            case EQUALS:
                return (b_height / width) / totalCount;
            // P (X != v)
            case NOT_EQUALS:
                return 1.0 - (b_height / width) / totalCount;
            // P (X > v)
            case GREATER_THAN:
                // 计算当前bucket中大于v的部分
                double b_part = (b_right - v) / width * b_freq;
                // 计算所有右侧bucket的频率之和
                for (int i = bucketIndex + 1; i < buckets; i++) {
                    b_part += (double)bucketCounts[i] / totalCount;
                }
                return b_part;
            // P (X >= v)
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            // P (X < v)
            case LESS_THAN:
                // 计算当前bucket中小于v的部分
                double b_part2 = (v - b_left) / width * b_freq;
                // 计算所有左侧bucket的频率之和
                for (int i = 0; i < bucketIndex; i++) {
                    b_part2 += (double) bucketCounts[i] / totalCount;
                }
                return b_part2;
            // P (X <= v)
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                return -1.0; // Unknown operator

        }

    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0 / buckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IntHistogram (total tuples = ").append(totalCount).append(")\n");

        for (int i = 0; i < buckets; i++) {
            double b_left = min + i * width;
            double b_right = b_left + width - 1;  // -1 保证右边界不重叠
            if (i == buckets - 1) {
                b_right = max; // 最后一个桶用 max 收尾，防止精度误差
            }

            sb.append(String.format(
                    "Bucket %2d: [%6.2f - %6.2f] count=%4d (%.2f%%)\n",
                    i, b_left, b_right, bucketCounts[i],
                    totalCount == 0 ? 0.0 : (bucketCounts[i] * 100.0 / totalCount)
            ));
        }
        return sb.toString();
    }
}
