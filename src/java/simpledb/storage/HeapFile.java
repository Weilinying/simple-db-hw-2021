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
        // not necessary for lab1
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
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
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

