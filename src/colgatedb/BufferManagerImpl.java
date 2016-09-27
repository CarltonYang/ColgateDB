package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

import java.awt.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

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
public class BufferManagerImpl implements BufferManager {

    private boolean allowEvictDirty = false;  // a flag indicating whether a dirty page is candidate for eviction
    private int numPages;
    private DiskManager dm;
    private HashMap<PageId, Frame> frameMap;
    private DoublyLinkedList clocklist;

    /**
     * Construct a new buffer manager.
     *
     * @param numPages maximum size of the buffer pool
     * @param dm       the disk manager to call to read/write pages
     */
    public BufferManagerImpl(int numPages, DiskManager dm) {
        this.numPages = numPages;
        this.dm = dm;
        frameMap = new HashMap<PageId, Frame>(numPages);
        clocklist = new DoublyLinkedList(numPages);
    }

    @Override
    public synchronized Page pinPage(PageId pid, PageMaker pageMaker) {
        Frame tempFrame= null;
        if (frameMap.containsKey(pid)) {
            tempFrame = frameMap.get(pid);
            int pinCountRemoveFrame= tempFrame.getPinCount();
            tempFrame.incrementPin();
            frameMap.put(pid, tempFrame);
            if (pinCountRemoveFrame==0){
                clocklist.removeFrame(tempFrame);
            }
            return getPage(pid);
        } else {
            if (frameMap.size()==numPages){
                evictPage();
            }
            Page newPage = dm.readPage(pid, pageMaker);
            frameMap.put(pid, new Frame(newPage));
            return newPage;
        }

    }

    @Override
    public synchronized void unpinPage(PageId pid, boolean isDirty) {
        if (frameMap.containsKey(pid)) {
            Frame tempFrame = frameMap.get(pid);
            if (tempFrame.getPinCount() > 0) {
                tempFrame.decrementPin();
                tempFrame.setDirty(isDirty);
                if (tempFrame.getPinCount()==0){
                    //clocklist.moveFrameToHead(tempFrame);
                    clocklist.addFrameToList(tempFrame);
                }
            } else {
                throw new BufferManagerException("Pincount of the page is already 0!");
            }
        } else {
            throw new BufferManagerException("Page associated with this pid is not in cache!");
        }
    }

    @Override
    public synchronized void flushPage(PageId pid) {
        Frame tempFrame = frameMap.get(pid);
        if (tempFrame.isDirty()) {
            dm.writePage(tempFrame.getPage());
        }
    }

    @Override
    public synchronized void flushAllPages() {
        for (PageId key : frameMap.keySet()) {
            flushPage(key);
        }
    }

    @Override
    public synchronized void evictDirty(boolean allowEvictDirty) {
        this.allowEvictDirty = allowEvictDirty;
    }

    @Override
    public synchronized void allocatePage(PageId pid) {
        dm.allocatePage(pid);
    }

    @Override
    public synchronized boolean isDirty(PageId pid) {
        if (frameMap.containsKey(pid)) {
            return frameMap.get(pid).isDirty();
        } else {
            return false;
        }
    }

    @Override
    public synchronized boolean inBufferPool(PageId pid) {
        return frameMap.containsKey(pid);
    }

    @Override
    public synchronized Page getPage(PageId pid) {
        if (frameMap.containsKey(pid)) {
            return frameMap.get(pid).getPage();
        } else {
            throw new BufferManagerException("Page is not in Buffer Manager!");
        }
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        frameMap.remove(pid);
    }


    /*
     * This function finds a candidate page for eviction upon request.
     * I choose LRU as my eviction algorithm.
     * My implementation of an efficient eviction strategy involves a doubly linked list with frame
     * as node and a hashmap with pid as key and frame as value. The idea is to use doubly linked list
     * for updating the queue and access the page through hashmap.
     *
     * Every page has a pid and when they are brought into buffer pool, they are kepted in a frame where
     * additional information is kept. Pincount is larger than 0 when a page is pinned and 0 when it is not
     * requested by any prgrame. We only keep pages with 0 pincount in the LRU queue: the page is put into
     * the head of the queue when some program unpins the page and the pincount becomes 0. Obviously, when
     * a page is in the queue but pinned again, we will remove it from the queue. Accessing a page in the
     * buffer pool is always constant time because of hashmap.
     *
     * In pinpage(), if the requested page is already in the buffer pool, then we simply access the page
     * with pid, update pincount and if pincount was 0 (meaning the the page
     * was in LRU queue) remove it from the queue. It is clear that access through hashmap and update incount
     * are both constant time operations. As for removing the frame from the list, we enjoy the benefit of
     * having a doubly linked list instead of a normal one. Here we do not have to linearly iterate through
     * all the nodes; we can simlpy connect the frames before and after the current frame through the prev and next
     * pointers inside the frame. Thus it is constant time as well.
     *
     * If the page is not in the buffer pool, then we need to bring it in. Assume that allowEvictDirty is true,
     * and there are frames in the queue.
     * Then we simply choose the tail of the queue and evict this page because only pages with pincount 0 are
     * allowed in the queue. This is constant time since there is a pointer to the tail of the queue.
     *
     * When allowEvictDirty is false, however, the runtime could be slightly slower since we have one queue
     * for both dirty and clean pages. We have no other choice but iterate through all of them from the least
     * recently used one (which is tail of the queue). The runtime is O(n) when every page in
     * the queue is dirty or the only clean one is the head. Nonetheless, n is not necessarily always the size of the
     * buffer pool since only pages with pincount 0 are allowed in the queue. The extreme case happens when all the
     * pages in the buffer pool are dirty with pincount of 0. In the worst case, the algorithm will have a runtime linear
     * to the size of the pool.
     *
     * Overall, the runtime should perform efficiently (constant time) when allowEvictDirty is true.
     */
    private synchronized void evictPage(){
        if (clocklist.getCurrSize()==0){
            throw new BufferManagerException("No unpinned page avaliable!");
        }
        Frame current= clocklist.getTail();
        while (current!=null){
            if (allowEvictDirty){
                flushPage(clocklist.getTail().getPage().getId());
                frameMap.remove(clocklist.getTail().getPage().getId());
                clocklist.removeTail();
                return;
            }
            else if (!allowEvictDirty){
                if (current.isDirty()){
                    current=current.getPrev();
                }
                else{
                    flushPage(current.getPage().getId());
                    frameMap.remove(current.getPage().getId());
                    clocklist.removeFrame(current);
                    return;
                }
            }
        }
        throw new BufferManagerException("All frames are dirty! Enable AllowEvictDirty and try again!");
    }
    /**
     * A frame holds one page and maintains state about that page. It is modified to serves as a Node in
     * doubly linked list and has prev and next.
     */
    private class Frame {
        private Page page;
        private int pinCount;
        public boolean isDirty;
        private Frame prev;
        private Frame next;

        public Frame(Page page) {
            this.page = page;
            this.pinCount = 1;   // assumes Frame is created on first pin -- feel free to modify as you see fit
            this.isDirty = false;
        }

        public Frame getPrev() {
            return prev;
        }

        public void setPrev(Frame prev) {
            this.prev = prev;
        }

        public Frame getNext() {
            return next;
        }

        public void setNext(Frame next) {
            this.next = next;
        }

        public int getPinCount() {
            return pinCount;
        }

        public void incrementPin() {
            this.pinCount++;
        }

        public void decrementPin() {
            if (this.pinCount > 0) {
                pinCount--;
            }
        }

        public void setDirty(boolean dirty) {
            if (!this.isDirty) {
                this.isDirty = dirty;
            }
        }

        public boolean isDirty() {
            return this.isDirty;
        }

        public Page getPage() {
            return this.page;
        }
    }

    /*
     * A sub class that implements an efficient queue for LRU
     */
    private class DoublyLinkedList {
        private final int size;
        private int currSize;
        private Frame head;
        private Frame tail;

        public DoublyLinkedList(int size) {
            this.size = size;
            currSize = 0;
        }

        public Frame getTail() {
            return tail;
        }

        public void setTail(Frame frame){
            this.tail=frame;
        }

        public Frame addFrameToList(Frame frame) {
            //Frame frame = new Frame(page);
            if (head == null) {
                head = frame;
                tail = frame;
                currSize = 1;
                return frame;
            } else if (currSize < size) {
                currSize++;
            } //else if (frame!=tail){
              //  tail = tail.getPrev();
              //  tail.setNext(null);
            //}
            frame.setNext(head);
            head.setPrev(frame);
            head = frame;
            return frame;
        }

        public void moveFrameToHead(Frame frame) {
            if (frame == null || frame == head) {
                return;
            }

            if (frame == tail) {
                tail = tail.getPrev();
                tail.setNext(null);
            }

            Frame prev = frame.getPrev();
            Frame next = frame.getNext();
            prev.setNext(next);

            if (next != null) {
                next.setPrev(prev);
            }

            frame.setPrev(null);
            frame.setNext(head);
            head.setPrev(frame);
            head = frame;
        }

        public void removeTail(){
            if (this.currSize>1) {
                Frame newtail = this.tail.getPrev();
                newtail.setNext(null);
                this.tail = newtail;
            }
            else{
                this.setCurrSize(0);
                this.setHead(null);
                this.setTail(null);
            }
        }

        public void removeFrame(Frame current)
        {
            if (current.getNext() ==  null &&current.getPrev() == null){
                this.setTail(null);
                this.setHead(null);
                //this.setCurrSize(0);
            }
            else if(current.getNext() ==  null)
            {
                Frame previous = current.getPrev();
                previous.setNext(null);
                current.setPrev(null);
                tail = previous;
            }
            else if (current.getPrev() == null)
            {
                Frame next = current.getNext();
                next.setPrev(null);
                current.setNext(null);
            }
            else
            {
                Frame next = current.getNext();
                Frame previous = current.getPrev();
                previous.setNext(next);
                next.setPrev(previous);
                current.setPrev(null);
                current.setNext(null);
            }
            currSize = size - 1;
            //return current;
        }

        public int getCurrSize() {
            return currSize;
        }

        public void setCurrSize(int currSize) {
            this.currSize = currSize;
        }

        public Frame getHead() {
            return head;
        }

        public void setHead(Frame head) {
            this.head = head;
        }
    }

}