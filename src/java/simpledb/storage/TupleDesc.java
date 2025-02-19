package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     *  the parameter used to hold tuple
     */
    private final TDItem[] tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // 通过将tuple转换为 List，然后获取 List 的迭代器的方式，来实现 iterator() 方法
        return Arrays.asList(tdItems).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // 初始化tdItems
        if (typeAr == null || fieldAr == null || typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("Type and field arrays must be non-null and of the same length");
        }
        tdItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // 初始化tdItems
        if (typeAr == null) {
            throw new IllegalArgumentException("Type array cannot be null");
        }
        tdItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            tdItems[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i < 0 || i >= tdItems.length){
            throw new NoSuchElementException("i is not a valid field reference.");
        }
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i < 0 || i >= tdItems.length){
            throw new NoSuchElementException("i is not a valid field reference.");
        }
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for(int i = 0; i < tdItems.length; i++){
            // If the field name is null, then only return it if name is also null.
            if (tdItems[i].fieldName == null) {
                if (name == null) {
                    return i;
                }
            } else if (tdItems[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("No field with a matching name is found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // 需要计算
        int size = 0;
        for(TDItem item: tdItems){
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // 获得td1和td2的type和name，然后new一个TupleDesc

        int numFields1 = td1.numFields();
        int numFields2 = td2.numFields();
        int totalFields = numFields1 + numFields2;

        Type[] mergedTypes = new Type[totalFields];
        String[] mergedStrings = new String[totalFields];

        for(int i = 0; i < numFields1; i++){
            mergedTypes[i] = td1.tdItems[i].fieldType;
            mergedStrings[i] = td1.tdItems[i].fieldName;
        }
        for(int j = 0; j < numFields2; j++){
            mergedTypes[numFields1 + j] = td2.tdItems[j].fieldType;
            mergedStrings[numFields1 + j] = td2.tdItems[j].fieldName;
        }

        return new TupleDesc(mergedTypes, mergedStrings);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // 重写Object类的equals方法，用于比较两个 TupleDesc 对象是否相等

        //Object 的 equals 方法接受任何类型的对象，但由于是重写，只想比较当前类的实例，故检查传入的对象是否是当前类的实例
        if (this.getClass().isInstance(o)) {
            // 将传入对象强制转换为TupleDesc类型
            TupleDesc td = (TupleDesc) o;

            // 比较两个TupleDesc对象的字段数量是否相等
            if (numFields() == td.numFields()) {
                // 遍历所有字段，逐一比较字段类型
                for (int i = 0; i < numFields(); ++i) {
                    // 如果任意字段类型不相等，直接返回false
                    if (!tdItems[i].fieldType.equals(td.tdItems[i].fieldType)) {
                        return false;
                    }
                }
                // 如果字段数量相同且字段类型逐一匹配，则返回true
                return true;
            }
        }
        // 如果传入对象不是TupleDesc的实例，或者字段数量不一致，返回false
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // throw new UnsupportedOperationException("unimplemented");


        // 初始化哈希值为1，通常使用非零值作为初始哈希值
        int hash = 1;

        // 遍历所有字段，逐一计算它们对哈希值的贡献
        for (int i = 0; i < tdItems.length; i++) {
            // 使用31作为乘数，是一种标准做法，可以有效减少哈希冲突
            // 如果字段类型 (fieldType) 为 null，则返回0；否则调用字段类型的 hashCode 方法
            hash = 31 * hash + (tdItems[i].fieldType == null ? 0 : tdItems[i].fieldType.hashCode());
        }

        // 返回最终计算的哈希值
        return hash;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // Append the field type, then the field name in parentheses.
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < tdItems.length; i++){
            sb.append(tdItems[i].fieldType);
            sb.append("(");
            sb.append(tdItems[i].fieldName);
            sb.append(")");
            // If this isn't the last field, add a comma and a space.
            if (i < tdItems.length - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
