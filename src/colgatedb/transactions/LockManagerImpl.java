package colgatedb.transactions;

import colgatedb.page.PageId;
import colgatedb.page.SimplePageId;

import java.util.*;
import java.util.concurrent.locks.Lock;

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
public class LockManagerImpl implements LockManager {

    private HashMap<PageId,LockTableEntry> HLTE;
    private HashMap<TransactionId, List<PageId>> HTP;

    public LockManagerImpl() {
        HLTE = new HashMap<PageId, LockTableEntry>();
        HTP = new HashMap<TransactionId, List<PageId>>();
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (!HLTE.containsKey(pid)){
            LockTableEntry temp = new LockTableEntry();
            HLTE.put(pid, temp);
        }
        LockTableEntry current = HLTE.get(pid);
        current.addRequest(tid,perm);

        boolean waiting = true;
        while (waiting) {
            synchronized (this) {
                // check if lock of requested page is available
                if (current.isNotLocked(tid, perm)) {
                    // it's not in use, so we can take it!
                    waiting = false;
                    current.addLockHolder(tid, perm);
                    addPid(tid, pid);
                }
                if (waiting) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {}
                }
            }
        }
    }

    @Override
    public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        if (HLTE.containsKey(pid)){
            LockTableEntry temp = HLTE.get(pid);
            if (temp.containsTid(tid)){
                if (temp.matchLockType(perm)){
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        LockTableEntry current = HLTE.get(pid);
        if (current == null || !current.containsTid(tid) ){
            throw new LockManagerException("This tid does not hold lock on this page!");
        }
        current.releaseLock(tid);
        this.notifyAll();
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        if (!HTP.containsKey(tid)){
            throw new LockManagerException("this Tid does not hold any pages!");
        }
        return HTP.get(tid);
    }

    @Override
    public synchronized List<TransactionId> getTidsForPage(PageId pid) {
        return new ArrayList<TransactionId>(HLTE.get(pid).getLockHolders());
    }

    private synchronized void addPid(TransactionId tid, PageId pid){
        if (HTP.containsKey(tid)){
            List<PageId> temp= HTP.get(tid);
            temp.add(pid);
            HTP.put(tid,temp);
        } else {
            List<PageId> newAdd = new LinkedList<PageId>();
            newAdd.add(pid);
            HTP.put(tid, newAdd);
        }
    }

}
