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
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        // 获取 catalog 文件所在的目录
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            // 按行读取文件内容
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                // 获取表名-format name
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                // 获取（）中间的field type
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim(); // trim()只会去掉string开头和结尾的空格，不会删除中间的空格
                String[] els = fields.split(",");

                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";

                for (String e : els) {
                    String[] els2 = e.trim().split(" "); // 拆成["field", "type"]
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim(); // 一张表里只有一个primaryKey
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }

                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]); // 使用 new String[0] 让 Java 自动创建正确大小的数组
                TupleDesc t = new TupleDesc(typeAr, namesAr);

                // 创建 HeapFile（文件存储），数据文件命名为 表名.dat
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

