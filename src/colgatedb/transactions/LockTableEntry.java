package colgatedb.transactions;

import colgatedb.page.Page;

import java.util.*;

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

/**
 * Represents the state associated with the lock on a particular page.
 */
public class LockTableEntry {

    // some suggested private instance variables; feel free to modify
    private Permissions lockType;             // null if no one currently has a lock
    private HashSet<TransactionId> lockHolders;   // a set of txns currently holding a lock on this page
    private List<LockRequest> requests;       // a queue of outstanding requests

    public LockTableEntry() {
        lockType = null;
        lockHolders = new HashSet<>();
        requests = new LinkedList<>();
        // you may wish to add statements here.
    }

    /*
     *   check if two permissions match
     */
    public synchronized boolean matchLockType(Permissions perm){
        if (lockType == null){
            return false;
        }

        if (lockType == Permissions.READ_WRITE && perm!=null){
            return true;
        } else if (lockType == Permissions.READ_ONLY){
            return perm==Permissions.READ_ONLY;
        }
        return false;
    }

    /*
     *   check if this page is locked
     */
    public synchronized boolean isNotLocked(TransactionId tid, Permissions perm) {
        LockRequest temp = new LockRequest(tid, perm);
        if (lockType != Permissions.READ_WRITE && perm == Permissions.READ_ONLY && isNext(temp)) {
            return true;
        } else if (perm == Permissions.READ_WRITE && isNext(temp) && exclusiveAvailable(tid)) {
            return true;
        } else if (isupGrade(tid, perm)) {
            requests.remove(tid);
            LockRequest cutline = new LockRequest(tid, perm);
            requests.add(0,cutline);
            return true;
        } else{
            return false;
        }
    }

    /*
     *   check if this exclusive lock request can be granted
     */
    private synchronized boolean exclusiveAvailable(TransactionId tid){
        //if there is no lock holder or there is only one lock holder which is the same as this request,
        //grant the lock
        return (lockHolders.isEmpty()|| (lockHolders.size()==1)
                &&lockHolders.contains(tid) && lockType==Permissions.READ_WRITE);
    }

    /*
     *   check if this request is an upgrade of a previous shared lock
     */
    private synchronized boolean isupGrade(TransactionId tid, Permissions perm){
        if (lockHolders.size()==1 && lockHolders.contains(tid) && lockType==Permissions.READ_ONLY && perm== Permissions.READ_WRITE){
            return true;
        } else {
            return false;
        }
    }

    public synchronized boolean containsTid(TransactionId tid){
        return lockHolders.contains(tid);
    }

    private synchronized boolean isNext(LockRequest lockRequest){
        return requests.get(0).equals(lockRequest);
    }

    public synchronized void releaseLock(TransactionId tid){
        lockHolders.remove(tid);
        if (lockHolders.isEmpty()){
            lockType=null;
        }
    }

    public synchronized void addLockHolder(TransactionId tid, Permissions perm){
        requests.remove(new LockRequest(tid, perm));
        lockHolders.add(tid);
        lockType = perm;
    }

    public synchronized HashSet<TransactionId> getLockHolders(){
        return this.lockHolders;
    }

    public synchronized void addRequest(TransactionId tid, Permissions perm){
        LockRequest temp = new LockRequest(tid, perm);
        requests.add(temp);
    }

    public synchronized void removeRequest(TransactionId tid, Permissions perm){
        LockRequest temp = new LockRequest(tid, perm);
        requests.remove(temp);
    }
    /**
     * A class representing a single lock request.  Simply tracks the txn and the desired lock type.
     * Feel free to use this, modify it, or not use it at all.
     */
    private class LockRequest {
        public final TransactionId tid;
        public final Permissions perm;

        public LockRequest(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }

        public boolean equals(Object o) {
            if (!(o instanceof LockRequest)) {
                return false;
            }
            LockRequest otherLockRequest = (LockRequest) o;
            return tid.equals(otherLockRequest.tid) && perm.equals(otherLockRequest.perm);
        }

        public String toString() {
            return "Request[" + tid + "," + perm + "]";
        }
    }
}
