package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private final int maxPages;

    private final LinkedHashMap<PageId, Page> pageCache;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final LockerManager lockerManager = new LockerManager();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.maxPages = numPages;
        pageCache = new LinkedHashMap<>(16, 0.75f, true);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve(取回) the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted(驱逐) and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        // 在取页前阻塞加锁,避免因为“拿着 BufferPool 的 monitor 再去 wait”导致系统僵住
        try{
            lockerManager.acquire(tid, pid, perm);
        }catch (InterruptedException e) {
            throw new TransactionAbortedException();
        }


        // If the page is already cached, return it.
        if (pageCache.containsKey(pid)) {
            return pageCache.get(pid);
        }

        // If the buffer pool is full (i.e. number of pages equals maxPages),
        // throw an exception because no eviction policy is implemented for this lab.
        if (pageCache.size() >= maxPages) {
            evictPage();
        }

        // Load the page from disk.
        // Retrieve the DbFile that contains the requested page using the tableId from pid.
        DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbfile.readPage(pid);

        // Cache the newly loaded page.
        pageCache.put(pid, page);

        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockerManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockerManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        synchronized (this){
            try{
                for (PageId pid : new ArrayList<>(pageCache.keySet())) {
                    Page page = pageCache.get(pid);
                    TransactionId dirtier = page.isDirty();

                    // 只处理“本事务修改过”的页
                    if (dirtier != null && dirtier.equals(tid)) {
                        if (commit){
                            // 提交：写回磁盘
                            flushPage(pid);
                            // 标记干净，不需要，flushPage里已经实现
                            // page.markDirty(false, null);

                            // use current page contents as the before-image
                            // for the next transaction that modifies this page.
                            page.setBeforeImage();
                        } else {
                            // 回滚：换成旧版本
                            Page beforeImage = page.getBeforeImage();
                            pageCache.put(pid, beforeImage);
                        }
                    }


                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                // 释放锁
                lockerManager.releaseAll(tid);
            }
        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // 1. 找到这张表对应的 DbFile（通常是 HeapFile）
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        // 2. 让文件层完成真正的插入，返回“被修改的页们”
        List<Page> dirtyPages = file.insertTuple(tid, t);

        // 3. 标脏 + 更新缓存（让后续访问看到最新版本）
        for (Page p : dirtyPages) {
            p.markDirty(true, tid);
            pageCache.put(p.getId(), p); // 覆盖旧版本（若已有）
       }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // 1. 从待删 tuple 的 RecordId 推回 tableId
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        // 2. 让文件层完成真正的删除
        List<Page> dirtyPages = file.deleteTuple(tid, t);

        // 3) 标脏 + 更新缓存
        for (Page p : dirtyPages) {
            p.markDirty(true, tid);
            pageCache.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        // 注意：LinkedHashMap 是 access-order 的，flushPage 中的 get() 会触发重排，
        // 直接遍历 keySet() 会导致 ConcurrentModificationException。
        // 解决：对 keys 做一份快照再遍历。
        for (PageId pid : new ArrayList<>(pageCache.keySet())) {
            flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageCache.get(pid);
        if (page == null) {
            return; // Page not in cache, nothing to flush
        }
        if (page.isDirty() != null) {
            // Get the DbFile that contains this page
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            // append an update record to the log, with
            // a before-image and after-image.
            TransactionId dirtier = page.isDirty();
            if (dirtier != null){
                Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
                Database.getLogFile().force();
            }
            // 写回磁盘
            dbFile.writePage(page);
            // // 清除脏标记
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        if (pageCache.isEmpty()) {
            throw new DbException("BufferPool is empty, cannot evict any page.");
        }

        PageId victimPid = null;

        // 从最老到最新（LRU），找“干净页”
        // 不使用pageCache.entrySet()，而是创建一个副本，避免在遍历过程中触发 LinkedHashMap 的结构性修改
        for (PageId pid : new ArrayList<>(pageCache.keySet())) {
            Page victimPage = pageCache.get(pid);
            if (victimPage.isDirty() == null) { // 找到一个干净页
                victimPid = pid;
                break;
            }
        }

        // 有干净页：直接驱逐
        if (victimPid != null) {
            // 从缓存中移除
            pageCache.remove(victimPid);
            return;
        }

        // 全是脏页：NO-STEAL -> 不允许驱逐，直接失败
        throw new DbException("All pages are dirty; cannot evict under NO-STEAL policy.");

    }

}
