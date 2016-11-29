package colgatedb.transactions;

import colgatedb.page.PageId;
import colgatedb.page.SimplePageId;
import com.sun.tools.internal.xjc.reader.gbind.Graph;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


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

    private HashMap<PageId,LockTableEntry> HLTE; //one locktableentry for each page
    private HashMap<TransactionId, HashSet<PageId>> HTP; //each txn has a list of pid that this txn locks
    private WFGraph wfGraph; //wait for graph for deadlock detection

    public LockManagerImpl() {
        HLTE = new HashMap<PageId, LockTableEntry>();
        HTP = new HashMap<TransactionId, HashSet<PageId>>();
        wfGraph = new WFGraph();
    }

    /*
     *  helper function to help initialize LockTableEntry Hashmap and WFGraph
     *  if necessary
     */
    private void initialize(PageId pid, TransactionId tid){
        synchronized (this.HLTE){
            if (!HLTE.containsKey(pid)){
                HLTE.put(pid,  new LockTableEntry());
            }
        }
        synchronized (this.wfGraph){
            if (!wfGraph.containsTid(tid)){
                wfGraph.addTid(tid, new HashSet<TransactionId>());
            }
        }
    }//

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {

        initialize(pid, tid);
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
                    wfGraph.removeTid(tid);
                }
                if (waiting) {
                    try {
                        wfGraph.addTid(tid, current.getLockHolders());
                        //delete itself from the dependency group, this may happen from upgrading the lock
                        wfGraph.removeDependency(tid);
                        if (wfGraph.hasCircle(tid)){
                            wfGraph.print();
                            current.removeRequest(tid, perm);
                            wfGraph.removeTid(tid);
                            throw new TransactionAbortedException();
                        }
                        this.wait();
                    } catch (InterruptedException e) {}
                }
            }
        }
        synchronized (this){
            this.notifyAll(); //to make sure multiple sharedlock can acquire at the same time
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
        HTP.get(tid).remove(pid);
        this.notifyAll();
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        if (!HTP.containsKey(tid)){
            throw new LockManagerException("this Tid does not hold any pages!");
        }
        return new ArrayList<>(HTP.get(tid));
    }

    @Override
    public synchronized List<TransactionId> getTidsForPage(PageId pid) {
        return new ArrayList<TransactionId>(HLTE.get(pid).getLockHolders());
    }

    private synchronized void addPid(TransactionId tid, PageId pid){
        HashSet<PageId> newAdd = new HashSet<PageId>();
        if (HTP.containsKey(tid)){
            newAdd= HTP.get(tid);
        }
        newAdd.add(pid);
        HTP.put(tid, newAdd);
    }

    private class WFGraph{
        /*
         *  private class used for deadlock detection
         */
        private HashMap<TransactionId, HashSet<TransactionId>> dependencyMap;

        public WFGraph(){
            dependencyMap = new HashMap<TransactionId, HashSet<TransactionId>>();
        }

        public synchronized void addTid(TransactionId tid, HashSet<TransactionId> dependencyTid){
            if (dependencyMap.containsKey(tid)){
                dependencyMap.get(tid).addAll(dependencyTid);
            } else{
                dependencyMap.put(tid, dependencyTid);
            }
        }

        public synchronized void removeTid(TransactionId tid){
            dependencyMap.remove(tid);
        }

        public synchronized void removeDependency(TransactionId tid){
            dependencyMap.get(tid).remove(tid);
        }

        public synchronized boolean containsTid(TransactionId tid){
            return dependencyMap.containsKey(tid);
        }

        // BFS to detect circle
        /*public synchronized boolean hasCircle(TransactionId tid){
            HashSet<TransactionId> visited = new HashSet<TransactionId>();
            LinkedList<TransactionId> queue = new LinkedList<TransactionId>();
            queue.add(tid);

            while (!(queue.isEmpty())) {
                TransactionId current = queue.remove();
                if (visited.contains(current)) {
                    return true;
                }
                visited.add(current);

                if (dependencyMap.containsKey(current) && !(dependencyMap.get(current).isEmpty())) {
                    Iterator<TransactionId> it = this.dependencyMap.get(current).iterator();
                    while (it.hasNext()) {
                        queue.add(it.next());
                    }
                }
            }
            return false;
        }*/

        // DFS to detect circle
        public synchronized boolean hasCircle(TransactionId tid2){
            HashSet<TransactionId> visited = new HashSet<TransactionId>();
            LinkedList<TransactionId> queue = new LinkedList<TransactionId>();
            for(TransactionId tid : dependencyMap.keySet()) {
                if(hasCircleHelper(visited,queue,tid))
                    return true;
            }
            return false;
        }

        private synchronized boolean hasCircleHelper(HashSet<TransactionId> visited,
                                                     LinkedList<TransactionId> queue,
                                                     TransactionId tid){
            HashSet<TransactionId> tempvisited= new HashSet<TransactionId>();
            if(!visited.contains(tid)) {
                tempvisited.add(tid);
                queue.add(tid);
                //for all adjacent vertices
                HashSet<TransactionId> temp= dependencyMap.get(tid);
                if (temp==null){
                    return false;
                }
                for(TransactionId txnId:temp) {
                    //if the adjacent node is not visited yet
                    if(!visited.contains(txnId)) {
                        tempvisited.addAll(visited);
                        if(hasCircleHelper(tempvisited, queue, txnId))
                            return true;
                    } else if(queue.contains(txnId)){
                        return true;
                    }
                }
            }
            queue.remove(tid);
            return false;
        }

        public synchronized void print(){
            for (TransactionId name: dependencyMap.keySet()){
                String key =name.toString();
                String value = dependencyMap.get(name).toString();
                System.out.println(key + " " + value);
            }
        }
    }

}
