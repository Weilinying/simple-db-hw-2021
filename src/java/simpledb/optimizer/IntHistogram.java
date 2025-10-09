package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int buckets;
    private final int min;
    private final int max;
    private final int width;       // 每个桶覆盖的“整数个数”（至少为1）
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
        // 保证桶宽至少为 1 个整数，避免域大小小于桶数时等值概率被放大
        int domain = Math.max(0, max - min + 1); // 一共有多少个不同整数值
        this.width = Math.max(1, (int) Math.ceil(domain / (double) buckets)); // “算平均每个桶该装多少个数”，并向上取整
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

        // 找到这个 value 属于哪个 bucket（整数除法，最后一个桶兜底）
        int bucketIndex = (v - min) / width;
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

        // 计算 bucket 信息
        int bucketIndex = (v - min) / width;
        if (bucketIndex >= buckets) {
            bucketIndex = buckets - 1; // edge case: v == max
        }

        int b_left = min + bucketIndex * width;        // 当前桶的左边界（包含）
        int b_right = b_left + width;                   // 当前桶的右边界（不包含，半开区间）
        int b_height = bucketCounts[bucketIndex];       // 当前桶内的频数
        double b_freq = (double) b_height / totalCount; // 当前桶内的频率

        switch (op){
            // P (X = v))，假设桶内对每个整数均匀分布
            case EQUALS:
                return ((double) b_height / width) / totalCount;
            // P (X != v)
            case NOT_EQUALS:
                return 1.0 - (((double) b_height / width) / totalCount);
            // P (X > v)
            case GREATER_THAN: {
                double part = 0.0;
                // 当前桶 > v 的整数个数占桶宽比例（不含 v 本身）
                int greaterInBucket = Math.max(0, b_right - (v + 1));
                part += b_freq * ((double) greaterInBucket / width);
                // 右侧所有桶的频率之和
                for (int i = bucketIndex + 1; i < buckets; i++) {
                    part += (double) bucketCounts[i] / totalCount;
                }
                return part;
            }
            // P (X >= v)
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            // P (X < v)
            case LESS_THAN: {
                double part = 0.0;
                // 当前桶 < v 的整数个数占桶宽比例（不含 v 本身）
                int lessInBucket = Math.max(0, v - b_left);
                part += b_freq * ((double) lessInBucket / width);
                // 左侧所有桶的频率之和
                for (int i = 0; i < bucketIndex; i++) {
                    part += (double) bucketCounts[i] / totalCount;
                }
                return part;
            }
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
            int b_left = min + i * width;
            int b_right = Math.min(max, b_left + width - 1);  // 包含右边界，最后一个桶对齐 max

            sb.append(String.format(
                    "Bucket %2d: [%d - %d] count=%4d (%.2f%%)\n",
                    i, b_left, b_right, bucketCounts[i],
                    totalCount == 0 ? 0.0 : (bucketCounts[i] * 100.0 / totalCount)
            ));
        }
        return sb.toString();
    }
}
