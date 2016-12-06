package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;
import colgatedb.page.SlottedPage;
import colgatedb.transactions.*;

import java.util.*;

/**
 * ColgateDB
 *
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
public class AccessManagerImpl implements AccessManager {

    private boolean force = true;  // indicates whether force policy should be used
    private HashMap<TransactionId, List<PageId>> txnRecord;
    private LockManagerImpl lockManager;
    private BufferManager bufferManager;
    /**
     * Initialize the AccessManager, which includes creating a new LockManager.
     * @param bm buffer manager through which all page requests should be made
     */
    public AccessManagerImpl(BufferManager bm) {
        lockManager= new LockManagerImpl();
        txnRecord= new HashMap<TransactionId, List<PageId>>();
        bufferManager=bm;
        bm.evictDirty(false);
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        lockManager.acquireLock(tid, pid, perm);
    }

    @Override
    public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        return lockManager.holdsLock(tid,pid,perm);
    }

    @Override
    public void releaseLock(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    @Override
    public Page pinPage(TransactionId tid, PageId pid, PageMaker pageMaker) {
        synchronized (this.txnRecord) {
            if (txnRecord.containsKey(tid)) {
                txnRecord.get(tid).add(pid);
            } else {
                LinkedList<PageId> temp = new LinkedList<PageId>();
                temp.add(pid);
                txnRecord.put(tid, temp);
            }
        }
        Page pinnedPage = bufferManager.pinPage(pid, pageMaker);
        return pinnedPage;
    }

    @Override
    public void unpinPage(TransactionId tid, Page page, boolean isDirty) {
        synchronized (this.txnRecord) {
            txnRecord.get(tid).remove(page.getId());
        }
        bufferManager.unpinPage(page.getId(),isDirty);
    }

    @Override
    public void allocatePage(PageId pid) {
        bufferManager.allocatePage(pid);
    }

    @Override
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    @Override
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (lockManager.getPagesForTid(tid) == null) {
            return;
        } else {
            Iterator<PageId> pageIdList = lockManager.getPagesForTid(tid).iterator();
            //Iterator<PageId> pageIdList = this.txnRecord.get(tid).iterator();
            while (pageIdList.hasNext()) {
                PageId pid = pageIdList.next();
                if (bufferManager.inBufferPool(pid)) {
                if (commit) {
                    if (bufferManager.isDirty(pid) && force){bufferManager.flushPage(pid);}
                    bufferManager.getPage(pid).setBeforeImage();
                } else {
                    if (bufferManager.isDirty(pid)) {
                        bufferManager.discardPage(pid);
                    } else {
                        while (txnRecord.get(tid).contains(pid)) {
                            unpinPage(tid, bufferManager.getPage(pid), false);
                            }
                        }
                    }
                }
            }
            while(!lockManager.getPagesForTid(tid).isEmpty()){
                releaseLock(tid,lockManager.getPagesForTid(tid).get(0));
            }
        }
    }


    @Override
    public void setForce(boolean force) {
        // you do NOT need to implement this for lab10.  this will be changed in a later lab.
        this.force=force;
    }
}
