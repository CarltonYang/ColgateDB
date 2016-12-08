package colgatedb.logging;

import colgatedb.BufferManager;
import colgatedb.Database;
import colgatedb.DiskManager;
import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.SimplePageId;
import colgatedb.transactions.TransactionId;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */
public class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        System.out.println("-------------- PRINT OF LOG FILE -------------- ");
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                    Page afterImg = LogFileImpl.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFileImpl.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {
         // this will be implemented in a later lab
        LinkedList<Page> backwardQueue = new LinkedList<Page>();
        readOnlyLog.seek(0);
        long currentOffset = readOnlyLog.getFilePointer();
        readOnlyLog.readLong();
        while (readOnlyLog.getFilePointer()<readOnlyLog.length()){
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    if (tidToRollback.getId()==tid){
                        Database.getLogFile().logAbort(tidToRollback.getId());
                    }
                    break;
                case LogType.COMMIT_RECORD:
                    if (tidToRollback.getId()==tid){
                        throw new IOException("Record was a commit");
                    }
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                    if (tidToRollback.getId()==tid){
                        backwardQueue.add(0,beforeImg);
                    }
                    LogFileImpl.readPageData(readOnlyLog);  // after image
                    break;
                case LogType.CLR_RECORD:
                    LogFileImpl.readPageData(readOnlyLog);  // after image
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    for (int i = 0; i < count; i++) {
                        readOnlyLog.readLong();
                    }
                    break;
                default:
                    break;
            }
            readOnlyLog.readLong();
            //System.out.println(readOnlyLog.getFilePointer());
            print();
        }

        for (Page beforeImg : backwardQueue){
            reset(tidToRollback,beforeImg);
        }
    }

    private void reset(TransactionId tidToRollback, Page beforeImg) throws IOException{
        Database.getDiskManager().writePage(beforeImg);
        Database.getBufferManager().discardPage(beforeImg.getId());
        Database.getLogFile().logCLR(tidToRollback, beforeImg);
    }

    private void rewrite(Page Img) throws IOException{
        Database.getDiskManager().writePage(Img);
        Database.getBufferManager().discardPage(Img.getId());
    }
    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {
         // this will be implemented in a later lab

        HashSet<Long> losers = new HashSet<Long>();

        long lastCheckPt= 8;
        readOnlyLog.seek(0);
        readOnlyLog.readLong();
        while (readOnlyLog.getFilePointer()<readOnlyLog.length()){
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    losers.add(tid);
                    break;
                case LogType.ABORT_RECORD:
                    losers.remove(tid);
                    break;
                case LogType.COMMIT_RECORD:
                    losers.remove(tid);
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                    LogFileImpl.readPageData(readOnlyLog);  // after image
                    break;
                case LogType.CLR_RECORD:
                    LogFileImpl.readPageData(readOnlyLog);  // after image
                    break;
                case LogType.CHECKPOINT_RECORD:
                    lastCheckPt = readOnlyLog.getFilePointer()-12 ;
                    int count = readOnlyLog.readInt();
                    HashSet<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    losers= tids;
                    break;
                default:
                    break;
            }
            readOnlyLog.readLong();
        }




        readOnlyLog.seek(lastCheckPt);
        while (readOnlyLog.getFilePointer()<readOnlyLog.length()){
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    losers.add(tid);
                    break;
                case LogType.ABORT_RECORD:
                    losers.remove(tid);
                    break;
                case LogType.COMMIT_RECORD:
                    losers.remove(tid);
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                    Page afterImg = LogFileImpl.readPageData(readOnlyLog);  // after image
                    rewrite(afterImg);
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFileImpl.readPageData(readOnlyLog);  // after image
                    Database.getDiskManager().writePage(afterImg);
                    //Database.getBufferManager().flushPage(afterImg.getId());
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    for (int i = 0; i < count; i++) {
                        readOnlyLog.readLong();
                    }
                    break;
                default:
                    break;
            }
            readOnlyLog.readLong();
        }

        LinkedList<Long> backwardQueue= new LinkedList<Long>();
        readOnlyLog.seek(0);
        readOnlyLog.readLong();
        while (readOnlyLog.getFilePointer()<readOnlyLog.length()){
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    if (losers.contains(tid)) {
                        Long currentOffSet= readOnlyLog.getFilePointer()-12;
                        backwardQueue.add(0,currentOffSet);
                        //typeMap.put(tid, LogType.BEGIN_RECORD);
                    }
                    break;
                case LogType.UPDATE_RECORD:
                    if (losers.contains(tid)){
                        Long currentOffSet= readOnlyLog.getFilePointer()-12;
                        backwardQueue.add(0,currentOffSet);
                    }
                    Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                    LogFileImpl.readPageData(readOnlyLog);  // after image
                    break;
                case LogType.CLR_RECORD:
                    LogFileImpl.readPageData(readOnlyLog);  // after image
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    for (int i = 0; i < count; i++) {
                        readOnlyLog.readLong();
                    }
                    break;
                default:
                    break;
            }
            readOnlyLog.readLong();
        }

        for (Long Offset : backwardQueue){
                readOnlyLog.seek(Offset);
                int type = readOnlyLog.readInt();
                long tid = readOnlyLog.readLong();
                switch (type){
                    case LogType.BEGIN_RECORD:
                        Database.getLogFile().logAbort(tid);
                        break;
                    case LogType.UPDATE_RECORD:
                        Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                        Database.getDiskManager().writePage(beforeImg);
                        Database.getLogFile().logCLR(tid, beforeImg);
                        LogFileImpl.readPageData(readOnlyLog);  // after image
                        break;
                    case LogType.CLR_RECORD:
                        LogFileImpl.readPageData(readOnlyLog);  // after image
                        break;
                    case LogType.CHECKPOINT_RECORD:
                        int count = readOnlyLog.readInt();
                        for (int i = 0; i < count; i++) {
                            readOnlyLog.readLong();
                        }
                        break;
                    default:
                        break;
                }
        }
    }

    private long findCheckPt() throws IOException{
        long lastCheckPt= 0;


        return lastCheckPt;
    }
}
