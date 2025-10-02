package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated(填充数据) with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    // Map from table id to the database file storing the table's data
    private final Map<Integer, DbFile> tables;

    // Map from table id to table name
    private final Map<Integer, String> tableNames;

    // Map from table id to primary key field name
    private final Map<Integer, String> pkeyFields;

    // Map from table name to table id (for quick lookup by name)
    // In many database operations, you might need to find a table based on its name rather than its id.
    // Also, if a table with the same name already exists in the database,
    // you can easily update it using this approach, as the key is the table's name.
    private final Map<String, Integer> nameToId;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // Using ConcurrentHashMap instead of HashMap, since HashMap is not thread-safe.
        // ConcurrentHashMap is specifically designed to handle concurrent access without needing explicit synchronization.
        tables = new ConcurrentHashMap<>();
        tableNames = new ConcurrentHashMap<>();
        pkeyFields = new ConcurrentHashMap<>();
        nameToId = new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // Check for null file or name as these are required.
        if (file == null) {
            throw new IllegalArgumentException("DbFile cannot be null");
        }
        if (name == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }

        int tableId = file.getId();
        tables.put(tableId, file);
        tableNames.put(tableId, name);
        pkeyFields.put(tableId, pkeyField);
        nameToId.put(name, tableId);

    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        if (name == null || !nameToId.containsKey(name)) {
            throw new NoSuchElementException("Table with name " + name + " does not exist.");
        }
        return nameToId.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        if(!tables.containsKey(tableid)){
            throw new NoSuchElementException("Table with id " + tableid + " does not exist.");
        }
        return tables.get(tableid).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        if(!tables.containsKey(tableid)){
            throw new NoSuchElementException("Table with id " + tableid + " does not exist.");
        }
        return tables.get(tableid);
    }

    public String getPrimaryKey(int tableid) {
        if(!tables.containsKey(tableid)){
            throw new NoSuchElementException("Table with id " + tableid + " does not exist.");
        }
        return pkeyFields.get(tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        return tables.keySet().iterator();
    }

    public String getTableName(int id) {
        if(!tableNames.containsKey(id)){
            throw new NoSuchElementException("Table with id " + id + " does not exist.");
        }
        return tableNames.get(id);
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        tables.clear();
        tableNames.clear();
        pkeyFields.clear();
        nameToId.clear();
    }

    /**
     * Reads the schema from a catalog file and registers tables in the database.
     * 作用：从一个 schema 文件中读取表的定义（表名、字段名、字段类型、主键），
     * 然后创建对应的 TupleDesc（表结构描述）和 HeapFile（数据文件对象），
     * 并将它们注册到 Catalog 中。
     *
     * @param catalogFile 文件路径，例如 "catalog.txt"
     * 文件内容格式示例：
     * Students (id int pk, name string, grade int)
     */
    public void loadSchema(String catalogFile) {
        String line = ""; // 存储每一行 schema 定义
        // 获取 catalog.txt 所在的文件夹路径，用于后面找到数据文件 .dat
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();

        try {
            // 打开文件并逐行读取
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                // 假设每行格式是：表名 (字段名 类型, 字段名 类型, ...)
                // 比如：Students (id int pk, name string, grade int)

                // 1️⃣ 解析表名：获取 "(" 前的部分
                String name = line.substring(0, line.indexOf("(")).trim();

                // 2️⃣ 提取括号内的字段定义
                // 例如：id int pk, name string, grade int
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();

                // 3️⃣ 按逗号分割多个字段
                String[] els = fields.split(",");

                // 存字段名、字段类型、主键
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";

                // 4️⃣ 遍历每个字段定义
                for (String e : els) {
                    // e 例如："id int pk"
                    String[] els2 = e.trim().split(" "); // 按空格切成 ["id", "int", "pk"]

                    // ---- 字段名 ----
                    names.add(els2[0].trim());

                    // ---- 字段类型 ----
                    // ⚠️ 这里就是你之前觉得“看不懂”的地方，其实就是把字符串映射为 Type 类型
                    // 如果是 int，就用 SimpleDB 定义好的 Type.INT_TYPE
                    // 如果是 string，就用 Type.STRING_TYPE
                    if (els2[1].trim().equalsIgnoreCase("int")) {
                        types.add(Type.INT_TYPE);
                    } else if (els2[1].trim().equalsIgnoreCase("string")) {
                        types.add(Type.STRING_TYPE);
                    } else {
                        // 如果遇到未知类型，直接报错退出
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }

                    // ---- 检查是否是主键（pk） ----
                    // 通常在 schema 里我们会用 pk 表示主键，比如 "id int pk"
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk")) {
                            primaryKey = els2[0].trim();
                        } else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }

                // 5️⃣ 把 ArrayList 转成数组，TupleDesc 构造函数需要数组类型
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);

                // 创建表结构描述对象（TupleDesc）
                TupleDesc t = new TupleDesc(typeAr, namesAr);

                // 6️⃣ 创建 HeapFile（数据文件对象）
                // 假设数据文件叫 Students.dat，就在和 catalog.txt 同一个目录下
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);

                // 7️⃣ 最后把这张表注册进 Catalog，系统才“认识”这张表
                addTable(tabHf, name, primaryKey);

                // 输出日志，确认表加载成功
                System.out.println("✅ Added table: " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry: " + line);
            System.exit(0);
        }
    }
}

