
package simpledb.storage;

import simpledb.common.Database;
import simpledb.transaction.TransactionId;
import simpledb.common.Debug;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

/*
LogFile implements the recovery subsystem of SimpleDb.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>
*/

/**
<p> The format of the log file is as follows:

<ul>

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

<li> Each log record ends with a long integer file offset representing
the position in the log file where the record began.

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>
*/
public class LogFile {

    final File logFile;
    private RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    long currentOffset = -1;//protected by this
//    int pageSize;
    int totalRecords = 0; // for PatchTest //protected by this

    final Map<Long,Long> tidToFirstLogRecord = new HashMap<>();

    /** Constructor.
        Initialize and back the log file with the specified file.
        We're not sure yet whether the caller is creating a brand new DB,
        in which case we should ignore the log file, or whether the caller
        will eventually want to recover (after populating the Catalog).
        So we make this decision lazily: if someone calls recover(), then
        do it, while if someone starts adding log file entries, then first
        throw out the initial log file contents.

        @param f The log file's name
    */
    public LogFile(File f) throws IOException {
	this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
                // public void run() { shutdown(); }
            // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    // we're about to append a log record. if we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
    void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
    }
    
    /** Write an abort record to the log for the specified tid, force
        the log to disk, and perform a rollback
        @param tid The aborting transaction.
    */
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getBufferPool()) {

            synchronized(this) {
                preAppend();
                //Debug.log("ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                rollback(tid);

                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    /** Write a commit record to disk for the specified tid,
        and force the log to disk.

        @param tid The committing transaction.
    */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log("COMMIT " + tid.getId());
        //should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /** Write an UPDATE record to disk for the specified tid and page
        (with provided         before and after images.)
        @param tid The transaction performing the write
        @param before The before image of the page
        @param after The after image of the page

        @see Page#getBeforeImage
    */
    public  synchronized void logWrite(TransactionId tid, Page before,
                                       Page after)
        throws IOException  {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int[] pageInfo = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int j : pageInfo) {
            raf.writeInt(j);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object[] idArgs = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = raf.readInt();
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException | InstantiationException e){
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning

    */
    public synchronized  void logXactionBegin(TransactionId tid)
        throws IOException {
        Debug.log("BEGIN");
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.print("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    //Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    /** Truncate any unneeded portion of the log to reduce its space
        consumption */
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();

                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                case UPDATE_RECORD:
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);

                    writePageData(logNew, before);
                    writePageData(logNew, after);
                    break;
                case CHECKPOINT_RECORD:
                    int numXactions = raf.readInt();
                    logNew.writeInt(numXactions);
                    while (numXactions-- > 0) {
                        long xid = raf.readLong();
                        long xoffset = raf.readLong();
                        logNew.writeLong(xid);
                        logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                    }
                    break;
                case BEGIN_RECORD:
                    tidToFirstLogRecord.put(record_tid,newStart);
                    break;
                }

                //all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e) {
                break;
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        //print();
    }

    /** Rollback the specified transaction, setting the state of any
        of pages it updated to their pre-updated state.  To preserve
        transaction semantics, this should not be called on
        transactions that have already committed (though this may not
        be enforced by this method.)

        @param tid The transaction to rollback
    */
    public void rollback(TransactionId tid)
        throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                preAppend();
                // some code goes here

                // 1. 找到该事务在日志文件里的起点
                Long logStart = tidToFirstLogRecord.get(tid.getId());
                if (logStart == null) {
                    // 这个事务可能没写过 UPDATE，或没 BEGIN 过；直接返回即可
                    return;
                }

                // 2. 得到这个事务写过的所有 UPDATE 的 before-image（正向扫描）
                List<Page> befores = new ArrayList<>();
                // 让接下来的读写操作从文件第 logStart 个字节处开始
                raf.seek(logStart);
                try {
                    while (true){
                        int type = raf.readInt(); // 记录类型
                        long record_tid = raf.readLong(); // 记录所属事务 id

                        if (type == UPDATE_RECORD) {
                            // UPDATE: [type][tid][before][after][startOfRecord]
                            // 读出 before/after，两者都会前进文件指针
                            Page before = readPageData(raf); // before-image
                            Page after = readPageData(raf); // after-image（rollback不需要，但必须读出来推进指针）
                            long _recStart = raf.readLong(); // 读出startOfRecord（rollback不需要，但必须读出来推进指针），这个和raf.readLong()作用一样，只是会在recover中用于反向扫描

                            if (record_tid == tid.getId()) {
                                // 只收集属于该事务的 UPDATE 的 before-image
                                befores.add(before);
                            }
                        }
                        else if (type == CHECKPOINT_RECORD) {
                            // CHECKPOINT: [type][tid][n][(tid,firstOffset)*n][startOfRecord]，格式跟别的不一样
                            int n = raf.readInt(); // 读出活跃事务数量 n
                            for (int i = 0; i < n; i++) {
                                raf.readLong(); // 读出活跃事务 id（rollback不需要，但必须读出来推进指针）
                                raf.readLong(); // 读出活跃事务的 firstOffset（rollback不需要，但必须读出来推进指针）
                            }
                            raf.readLong(); // 读出startOfRecord（rollback不需要，但必须读出来推进指针）
                        }
                        else{
                            // BEGIN/COMMIT/ABORT 等：格式都是 [type][tid][startOfRecord]
                            raf.readLong(); // 读出startOfRecord（rollback不需要，但必须读出来推进指针）
                        }
                    }
                }
                catch (EOFException e) {
                    // 读到文件末尾就停
                }

                // 3. 反向写回这些 before-image
                for (int i = befores.size() - 1; i >= 0; i--) {
                    Page before = befores.get(i);
                    PageId pid = before.getId();

                    // 覆盖磁盘页（物理 UNDO）
                    Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(before);

                    // 丢掉缓冲池里的该页，避免旧脏页再次刷盘覆盖回滚结果
                    Database.getBufferPool().discardPage(pid);
                }
            }




        }
    }

    /** Shutdown the logging system, writing out whatever state
        is necessary so that start up can happen quickly (without
        extensive recovery.)
    */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /** Recover the database system by ensuring that the updates of
        committed transactions are installed and that the
        updates of uncommitted transactions are not installed.
    */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                // some code goes here

                // 0. 数据结构
                // winners: 明确 COMMIT 的事务
                final Set<Long> winners = new HashSet<>();
                // losers: 扫描结束时仍未 COMMIT/ABORT 的事务
                final Set<Long> losers  = new HashSet<>();

                // 为了执行“先 UNDO 后 REDO”，我们缓存各事务遇到的 UPDATE 的前/后镜像
                final Map<Long, List<Page>> beforePagesByTid = new HashMap<>();
                final Map<Long, List<Page>> afterPagesByTid  = new HashMap<>();

                // 1. 找到最后一个 CHECKPOINT 位置
                raf.seek(0);
                long checkpointOffset = raf.readLong();

                // 我们要计算真正的 REDO 起点 startPos：
                //  - 没有 checkpoint：从 LONG_SIZE(=8) 开始；
                //  - 有 checkpoint：如果 checkpoint 时活跃事务数 > 0，
                //      从这些活跃事务的 firstLogRecord 的最小值开始；
                //    否则（活跃数 == 0），从 checkpoint 记录本身开始。
                long startPos;

                if (checkpointOffset == NO_CHECKPOINT_ID) {
                    // 没有 checkpoint => 从文件头后面 8 字节开始（跳过“checkpoint 指针”）
                    startPos = LONG_SIZE;
                }
                else{
                    // 有checkpoint => 先从 checkpointOffset 处读出活跃事务列表
                    raf.seek(checkpointOffset);

                    int type = raf.readInt(); // 理论上应该 == CHECKPOINT_RECORD
                    raf.readLong(); // 跳过 tid 占位符
                    int n = raf.readInt(); // 活跃事务数

                    long minFirst = Long.MAX_VALUE;   // 用来找“最早的 firstLogRecord”

                    for (int i = 0; i < n; i++) {
                        long activeTid = raf.readLong();     // 活跃事务 id
                        long firstLogRecord = raf.readLong(); // 该事务的 firstLogRecord

                        // 这些事务在 checkpoint 时刻是活跃的，先假设是“loser”，
                        // 后续正向扫描时遇到 COMMIT/ABORT 再把它们从 losers 移除。
                        losers.add(activeTid);

                        // rollback 需要 firstLogRecord：补齐到 tidToFirstLogRecord
                        tidToFirstLogRecord.put(activeTid, firstLogRecord);

                        // 用于计算 REDO 的真实起点（防止漏掉 checkpoint 之前的 UPDATE）
                        if (firstLogRecord < minFirst) {
                            minFirst = firstLogRecord;
                        }


                    }
                    raf.readLong(); // 跳过 checkpoint 记录末尾的 startOffset

                    // 计算 REDO 起点
                    if (n > 0) {
                        // 活跃事务数 > 0，从它们的 firstLogRecord 里选最早的
                        startPos = Math.min(checkpointOffset, minFirst);
                    }
                    else {
                        // 活跃事务数 == 0，从 checkpoint 记录本身开始
                        startPos = checkpointOffset;
                    }

                }

                // 2. 正向扫描
                raf.seek(startPos);
                while (true) {
                    try {
                        int type = raf.readInt(); // 记录类型
                        long tid = raf.readLong(); // 记录所属事务id

                        switch (type) {
                            case UPDATE_RECORD:{
                                // UPDATE: [type][tid][before][after][startOfRecord]
                                Page before = readPageData(raf); // before-image（redo不需要，但必须读出来推进指针）
                                Page after = readPageData(raf); // after-image
                                beforePagesByTid
                                        .computeIfAbsent(tid, k -> new ArrayList<>())
                                        .add(before);
                                afterPagesByTid
                                        .computeIfAbsent(tid, k -> new ArrayList<>())
                                        .add(after);

                                raf.readLong(); // 读出startOfRecord（redo不需要，但必须读出来推进指针）
                                break;
                            }
                            case BEGIN_RECORD:{
                                // BEGIN: [type][tid][startOfRecord]
                                long beginStartOffset = raf.readLong(); // 这条记录自身起点，也正是该事务的 firstOffset
                                losers.add(tid); // BEGIN 的事务先假设是“loser”，后续遇到 COMMIT/ABORT 再移除

                                // 若之前没从 checkpoint 拿到 firstOffset，就用 BEGIN 的起点补齐
                                tidToFirstLogRecord.putIfAbsent(tid, beginStartOffset);
                                break;
                            }
                            case COMMIT_RECORD:{
                                // COMMIT: [type][tid][startOfRecord]
                                raf.readLong(); // 读出startOfRecord（redo不需要，但必须读出来推进指针）
                                winners.add(tid); // 明确 COMMIT 的事务
                                losers.remove(tid); // 不再是“未完成”
                                break;
                            }
                            case ABORT_RECORD:{
                                // ABORT: [type][tid][startOfRecord]
                                raf.readLong(); // 读出startOfRecord（redo不需要，但必须读出来推进指针）
                                // 明确 ABORT 的事务不加入 winners
                                losers.remove(tid); // 不再是“未完成”
                                break;
                            }
                            case CHECKPOINT_RECORD:{
                                // CHECKPOINT: [type][tid][n][(tid,firstOffset)*n
                                raf.readLong(); // 跳过占位 tid
                                int n = raf.readInt(); // 读出活跃事务数量 n
                                for (int i = 0; i < n; i++) {
                                    long t = raf.readLong(); // 读出活跃事务 id（redo不需要，但必须读出来推进指针）
                                    long first = raf.readLong(); // 读出活跃事务的 firstOffset（redo不需要，但必须读出来推进指针）
                                    // 如果我们之前还没记录过某个事务的起始位置，就可以从这个 checkpoint 里“补齐”
                                    tidToFirstLogRecord.putIfAbsent(t, first);
                                }
                                raf.readLong(); // 读出startOfRecord（redo不需要，但必须读出来推进指针）
                                break;
                            }
                        }
                    }catch (EOFException eof) {
                        // 到达文件末尾，第一趟扫描结束
                        break;
                    }
                }

                // 4. REDO winners
                for (Long winnerTid : winners) {
                    List<Page> afters = afterPagesByTid.get(winnerTid);
                    if (afters == null || afters.isEmpty()) continue;
                    // 正序写回这些 after-image
                    for (Page after : afters) {
                        PageId pid = after.getId();
                        // 覆盖磁盘页（物理 REDO）
                        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(after);
                        // 丢掉缓冲池里的该页，避免旧脏页再次刷盘覆盖回滚结果
                        Database.getBufferPool().discardPage(pid);
                    }
                }

                // 3. UNDO losers
                for (Long loserTid : losers) {
                    List<Page> befores = beforePagesByTid.get(loserTid);
                    if (befores == null || befores.isEmpty()) continue;

                    // 逆序写回这些 before-image
                    for (int i = befores.size() - 1; i >= 0; i--) {
                        Page before = befores.get(i);
                        PageId pid = before.getId();
                        // 覆盖磁盘页（物理 UNDO）
                        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(before);
                        // 丢掉缓冲池里的该页，避免旧脏页再次刷盘覆盖回滚结果
                        Database.getBufferPool().discardPage(pid);
                    }
                }



                System.out.println("startPos = " + startPos);
                System.out.println("winners = " + winners);
                System.out.println("losers = " + losers);
                System.out.println("beforePagesByTid = " + beforePagesByTid.keySet());
                System.out.println("afterPagesByTid = " + afterPagesByTid.keySet());
            }
         }
    }

    /** Print out a human readable represenation of the log */
    public void print() throws IOException {
        long curOffset = raf.getFilePointer();

        raf.seek(0);

        System.out.println("0: checkpoint record at offset " + raf.readLong());

        while (true) {
            try {
                int cpType = raf.readInt();
                long cpTid = raf.readLong();

                System.out.println((raf.getFilePointer() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " + cpType);
                System.out.println((raf.getFilePointer() - LONG_SIZE) + ": TID " + cpTid);

                switch (cpType) {
                case BEGIN_RECORD:
                    System.out.println(" (BEGIN)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;
                case ABORT_RECORD:
                    System.out.println(" (ABORT)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;
                case COMMIT_RECORD:
                    System.out.println(" (COMMIT)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;

                case CHECKPOINT_RECORD:
                    System.out.println(" (CHECKPOINT)");
                    int numTransactions = raf.readInt();
                    System.out.println((raf.getFilePointer() - INT_SIZE) + ": NUMBER OF OUTSTANDING RECORDS: " + numTransactions);

                    while (numTransactions-- > 0) {
                        long tid = raf.readLong();
                        long firstRecord = raf.readLong();
                        System.out.println((raf.getFilePointer() - (LONG_SIZE + LONG_SIZE)) + ": TID: " + tid);
                        System.out.println((raf.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + firstRecord);
                    }
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                    break;
                case UPDATE_RECORD:
                    System.out.println(" (UPDATE)");

                    long start = raf.getFilePointer();
                    Page before = readPageData(raf);

                    long middle = raf.getFilePointer();
                    Page after = readPageData(raf);

                    System.out.println(start + ": before image table id " + before.getId().getTableId());
                    System.out.println((start + INT_SIZE) + ": before image page number " + before.getId().getPageNumber());
                    System.out.println((start + INT_SIZE) + " TO " + (middle - INT_SIZE) + ": page data");

                    System.out.println(middle + ": after image table id " + after.getId().getTableId());
                    System.out.println((middle + INT_SIZE) + ": after image page number " + after.getId().getPageNumber());
                    System.out.println((middle + INT_SIZE) + " TO " + (raf.getFilePointer()) + ": page data");

                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                    break;
                }

            } catch (EOFException e) {
                //e.printStackTrace();
                break;
            }
        }

        // Return the file pointer to its original position
        raf.seek(curOffset);
    }

    public  synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }

}
