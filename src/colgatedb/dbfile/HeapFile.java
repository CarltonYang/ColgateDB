package colgatedb.dbfile;

import colgatedb.AccessManager;
import colgatedb.BufferManager;
import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.page.*;
import colgatedb.transactions.Permissions;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.util.Iterator;
import java.util.NoSuchElementException;

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
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with SlottedPage. The format of SlottedPages is described in the javadocs
 * for SlottedPage.
 *
 * @see SlottedPage
 */
public class HeapFile implements DbFile {

    private final SlottedPageMaker pageMaker;   // this should be initialized in constructor
    private TupleDesc td;
    private int pageSize;
    private int tableid;
    private int numPages;
    private boolean nextSamePage;
    private boolean work;
    private Tuple current;
    //private Iterator<Tuple> currentIterator;
    /**
     * Creates a heap file.
     * @param td the schema for records stored in this heapfile
     * @param pageSize the size in bytes of pages stored on disk (needed for PageMaker)
     * @param tableid the unique id for this table (needed to create appropriate page ids)
     * @param numPages size of this heapfile (i.e., number of pages already stored on disk)
     */
    public HeapFile(TupleDesc td, int pageSize, int tableid, int numPages) {
        this.td=td;
        this.pageSize=pageSize;
        this.tableid=tableid;
        this.numPages=numPages;
        this.pageMaker=new SlottedPageMaker(td,pageSize);
        this.current=null;
        this.nextSamePage=false;
        this.work=true;
        //this.currentIterator=null;
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return this.numPages;
    }

    @Override
    public int getId() {
        return this.tableid;
    }

    @Override
    public TupleDesc getTupleDesc(){
        return this.td;
    }

    /*
     * this helper function either finds a page with a free slot or allocate a new
     * page and returns it
     */
    private PageId getFreePage(TransactionId tid) throws TransactionAbortedException{
        AccessManager accessManager= Database.getAccessManager();
        boolean justAcquired;

        for (int i = 0; i < this.numPages; i++) {
            PageId pid = new SimplePageId(this.tableid, i);
            justAcquired = !accessManager.holdsLock(tid, pid, Permissions.READ_ONLY);
            accessManager.acquireLock(tid, pid, Permissions.READ_ONLY);
            SlottedPage page = (SlottedPage) accessManager.pinPage(tid, pid,this.pageMaker);
            if (page.getNumEmptySlots() > 0) {
                accessManager.unpinPage(tid,page,false);
                return pid;
            }
            accessManager.unpinPage(tid, page, false);
            if (justAcquired){
                accessManager.releaseLock(tid, pid);
            }
        }
        SimplePageId newpid = new SimplePageId(tableid, numPages);
        //PageId creation outside synchronized block
        //because it needs to be accessed outside of the block and returned in the end
        synchronized (this) {
            accessManager.allocatePage(newpid);
            this.numPages++;
        }
        return newpid;
    }

    @Override
    public void insertTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        AccessManager accessManager= Database.getAccessManager();
        PageId newPageId= getFreePage(tid);
        accessManager.acquireLock(tid, newPageId, Permissions.READ_WRITE);
        SlottedPage newPage = (SlottedPage) accessManager.pinPage(tid, newPageId, this.pageMaker);
        newPage.insertTuple(t);
        accessManager.unpinPage(tid,newPage,true);//pinpage called inside getFreePage(), inserted a page -> dirty;
    }


    /**
     * Delete the tuple.
     * @param tid The transaction performing the update
     * @param t   The tuple to delete.  This tuple should be updated to reflect that
     *            it is no longer stored on any page.
     * @throws TransactionAbortedException
     * @throws DbException if the tuple does not have a valid record id
     */
    @Override
    public void deleteTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        if (t.getRecordId()==null){
            throw new DbException("The tuple does not have a valid record id!");
        }
        else{
            PageId pid= t.getRecordId().getPageId();
            Database.getAccessManager().acquireLock(tid, pid, Permissions.READ_WRITE);
            SlottedPage pagewithTuple= (SlottedPage) Database.getAccessManager().pinPage(tid, pid,this.pageMaker);
            pagewithTuple.deleteTuple(t);
            Database.getAccessManager().unpinPage(tid, pagewithTuple,true);//deleted a page -> dirty
        }
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * @see DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
        private boolean isOpened = false;
        private TransactionId tid;
        private int currentPage;
        private Iterator<Tuple> pageIterator;

        public HeapFileIterator(TransactionId tid) {
            this.tid=tid;
        }

        @Override
        public void open() throws TransactionAbortedException {
            if(!isOpened){
                currentPage=0;
                isOpened=true;
                this.pageIterator=getPageIterator(currentPage);
            }
        }

        /*
         * this helper function finds specific page and returns its iterator
         * so that only one page of data is store in the iterator not all of them
         */
        private Iterator<Tuple> getPageIterator(int currentPage) throws TransactionAbortedException, DbException {
            SimplePageId pid = new SimplePageId(tableid, currentPage);
            Database.getAccessManager().acquireLock(tid, pid, Permissions.READ_ONLY);
            SlottedPage page = (SlottedPage) Database.getAccessManager().pinPage(tid, pid,pageMaker);
            Iterator<Tuple> currentIterator=  page.iterator();
            Database.getAccessManager().unpinPage(tid, page, false);
            return currentIterator;
        }

        /*
         * this helper function unpins the page called earlier
         * new edit: currently unused anywhere
         */
        private void unpinPageUsedbyIterator(int pageNum){
            SimplePageId pid = new SimplePageId(tableid, pageNum);
            Page temp = Database.getBufferManager().getPage(pid);
            Database.getAccessManager().unpinPage(tid, temp,false);
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException {
            if (pageIterator == null|| !isOpened) { //return false if not open
                return false;
            } else if (!work){
                return true;
            } else if (pageIterator.hasNext()){ //return true if next is in the same page
                work=false;
                current=pageIterator.next();
                return true;
            }

            if (currentPage >= numPages()) { //return false if reaching end of all pages
                return false;
            } else { //try to find next in the next page
                int pageNum = currentPage;
                pageNum++;
                while (pageNum < numPages()) {
                    pageIterator = getPageIterator(pageNum);
                    if (pageIterator.hasNext()) {
                        current=pageIterator.next();
                        work=false;
                        currentPage=pageNum;
                        return true;
                    }
                    pageNum++;
                }
                currentPage=numPages()-1;
                return false;
            }
        }

        @Override
        public Tuple next() throws TransactionAbortedException, NoSuchElementException {
            if (!hasNext() || !isOpened) //return false if not open
                throw new NoSuchElementException("There is no more tuple in the heap file!");
            else {
                work=true;
                return current;
            }
        }

        @Override
        public void rewind() throws TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            isOpened = false;
            pageIterator = null;
        }
    }

}
