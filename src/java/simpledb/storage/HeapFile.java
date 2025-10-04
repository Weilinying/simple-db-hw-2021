package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;

    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.（磁盘上实际存储数据的文件，而不是在内存中的临时数据。）
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // 1. calculate the correct offset in the file
        // 能算出这页数据在文件中从哪一个字节开始
        int pageSize = BufferPool.getPageSize();
        int pageNo = pid.getPageNumber();
        int offset = pageSize * pageNo;

        byte[] data = new byte[pageSize]; // Buffer to hold the page data
        // 2. random access to the file
        // 打开磁盘上的表文件，允许“随机访问”（随时跳到文件的任意位置读写）
        try(RandomAccessFile raf = new RandomAccessFile(file, "r")){
            if(offset > raf.length()){ // raf.length()是文件的总字节数。文件的内容是从 0 到 raf.length() - 1 的位置
                throw new IllegalArgumentException("Requested page number " + pageNo + " exceeds file length.");
            }
            raf.seek(offset); // Move the file pointer to the correct offset
            raf.readFully(data); // 从文件当前位置开始，读取 data.length 字节的数据，填充 data 数组。
            return new HeapPage((HeapPageId) pid, data);
        }catch (IOException e){
            throw new IllegalArgumentException("Error reading page from file", e);
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here

        // 1. Page必须属于这个 HeapFile
        if (!(page instanceof HeapPage) || ((HeapPageId) page.getId()).getTableId() != getId()) {
            throw new IllegalArgumentException("Page does not belong to this HeapFile.");
        }

        // 2. 计算页在文件中的偏移量 pageNo × pageSize （第几个字节开始）
        int pageNo = page.getId().getPageNumber();
        int pageSize = BufferPool.getPageSize();
        long offset = (long) pageNo * pageSize; // 转成 long 防止溢出

        // 3. 把内存中的 Page 对象（Java 中的数据结构）转换成一段连续的 字节数组（byte[]），以便写入磁盘文件
        byte[] data = page.getPageData();
        if (data.length != pageSize) {
            throw new IllegalArgumentException("Page data length does not match page size.");
        }

        // 4. 随机访问文件，跳到正确的偏移量，写入磁盘
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(offset); // 将文件指针移动到该页起始位置
            raf.write(data); // 将页面数据写入文件
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        // 1. 找到一页有空闲槽位的页
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            // 通过 BufferPool 以可写权限拿页
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            // 检查该页是否有空闲槽位
            if (page.getNumEmptySlots() > 0) {
                // 在该页中插入元组
                page.insertTuple(t);
                // 返回修改过的页
                ArrayList<Page> modified = new ArrayList<>();
                modified.add(page);
                return modified;
            }
        }

        // 2. 如果没有页有空闲槽位，在文件末尾追加一个新空页，然后再插
        // 2.1 先把空页的字节写到文件最后，等于“扩容”文件
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(raf.length()); // 跳到文件末尾

            raf.write(HeapPage.createEmptyPageData()); // 写入一个空页的字节数据
        }

        // 2.2 新页创建好后，页号是 numPages() - 1
        int newPageNo = numPages() - 1;
        HeapPageId newPid = new HeapPageId(getId(), newPageNo);

        // 2.3 通过 BufferPool 以可写权限拿新页
        HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
        newPage.insertTuple(t);

        ArrayList<Page> modified = new ArrayList<>();
        modified.add(newPage);
        return modified;

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here

        // 1. 根据 tuple 自带的 RecordId 找到它在哪一页
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();

        // 检查这个页是否属于这个 HeapFile
        if (!(pid instanceof HeapPageId) || ((HeapPageId) pid).getTableId() != getId()) {
            throw new DbException("Tuple does not belong to this HeapFile.");
        }

        // 2. 通过 BufferPool 以可写权限拿页，让页删除该 tuple
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);

        ArrayList<Page> modified = new ArrayList<>();
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    // 对HeapFile进行迭代，以便能够一条一条地读取存储在 HeapPage 里的数据，就像 Java 的 Iterator 允许遍历 ArrayList 一样。
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private int currentPageIndex = 0;
            private Iterator<Tuple> tupleIterator; // 当前页的 Tuple 迭代器
            private boolean open = false; // 标志迭代器是否已打开

            // 辅助方法：获取指定页的 Tuple 迭代器
            // 找到HeapPage，遍历其中的Tuple
            private Iterator<Tuple> getTupleIterator(int pageIndex) throws DbException, TransactionAbortedException{
                if(pageIndex >= numPages()){
                    return null; // 如果页号超出范围，返回 null
                }
                PageId pid = new HeapPageId(getId(), pageIndex);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }
            // 定位到第一页，准备开始读
            @Override
            public void open() throws DbException, TransactionAbortedException {
                currentPageIndex = 0;
                tupleIterator = getTupleIterator(currentPageIndex);
                open = true;
            }

            // 看还有没有下一条 tuple
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!open) return false;
                // 当前页还有下一条 tuple
                if(tupleIterator != null && tupleIterator.hasNext()) return true;
                // 当前页没有了，尝试下一页
                while(currentPageIndex < numPages() - 1){
                    currentPageIndex++;
                    tupleIterator = getTupleIterator(currentPageIndex);
                    if(tupleIterator != null && tupleIterator.hasNext()) return true;
                }
                return false;
            }

            // 拿出下一条 tuple
            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more tuples");
                }
                return tupleIterator.next();
            }

            // 重新开始
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            // 关闭迭代器
            @Override
            public void close() {
                open = false;
                tupleIterator = null;
                currentPageIndex = numPages();
            }
        };
    }

}

