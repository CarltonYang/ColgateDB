package colgatedb.dbfile;

import colgatedb.BufferManager;
import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.page.*;
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

    private SlottedPage getFreePage(TransactionId tid) throws TransactionAbortedException{
        BufferManager buf= Database.getBufferManager();
        for (int i = 0; i < this.numPages; i++) {
            PageId pid = new SimplePageId(this.tableid, i);
            SlottedPage page = (SlottedPage) buf.pinPage(pid,this.pageMaker);
            if (page.getNumEmptySlots() > 0) {
                return page;
            }
        }
        SimplePageId pid = new SimplePageId(tableid, numPages);
        buf.allocatePage(pid);
        this.numPages++;
        SlottedPage page = (SlottedPage) buf.pinPage(pid,this.pageMaker);
        return page;
    }

    @Override
    public void insertTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        BufferManager buf= Database.getBufferManager();
        SlottedPage newPage= getFreePage(tid);
        newPage.insertTuple(t);
        PageId pid= newPage.getId();
        buf.unpinPage(pid,true);
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
            SlottedPage pagewithTuple= (SlottedPage) Database.getBufferManager().pinPage(pid,this.pageMaker);
            pagewithTuple.deleteTuple(t);
            Database.getBufferManager().unpinPage(pid,true);
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
        private Iterator<Tuple> pageIter;

        public HeapFileIterator(TransactionId tid) {
            this.tid=tid;
        }

        @Override
        public void open() throws TransactionAbortedException {
            if(!isOpened){
                currentPage=0;
                this.pageIter=getPageIterator(currentPage);
            }
        }

        private Iterator<Tuple> getPageIterator(int currentPage) throws TransactionAbortedException, DbException {
            SimplePageId pid = new SimplePageId(tableid, currentPage);
            SlottedPage page = (SlottedPage) Database.getBufferManager().pinPage(pid,pageMaker);
            return page.iterator();
        }

        private void unpinPageUsedbyIterator(int pageNum){
            SimplePageId pid = new SimplePageId(tableid, pageNum);
            Database.getBufferManager().unpinPage(pid,false);
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException {
            Iterator<Tuple> pageIterCurr= pageIter;
            if (pageIter == null) {
                return false;
            } else if (pageIter.hasNext())
                return true;
            if (currentPage >= numPages()) {
                return false;
            } else {
                int pgNo = currentPage;
                int curr= currentPage;
                unpinPageUsedbyIterator(currentPage);
                pgNo++;
                while (pgNo < numPages()) {
                    Iterator<Tuple> iter = getPageIterator(pgNo);
                    if (iter.hasNext()) {
                        unpinPageUsedbyIterator(pgNo);
                        getPageIterator(curr);
                        this.pageIter=pageIterCurr;
                        return true;
                    }
                    unpinPageUsedbyIterator(pgNo);
                    pgNo++;
                }
                getPageIterator(curr);
                this.pageIter=pageIterCurr;
                return false;
            }
        }

        @Override
        public Tuple next() throws TransactionAbortedException, NoSuchElementException {
            if (!hasNext())
                throw new NoSuchElementException("no tuple");
            else {
                if (pageIter.hasNext())
                    return pageIter.next();
                else {
                    unpinPageUsedbyIterator(currentPage);
                    currentPage++;
                    while (currentPage < numPages()) {
                        pageIter = getPageIterator(currentPage);
                        if (pageIter.hasNext()){
                            return pageIter.next();
                        }
                        unpinPageUsedbyIterator(currentPage);
                        currentPage++;
                    }
                }
            }
            throw new NoSuchElementException("no tuple");
        }

        @Override
        public void rewind() throws TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            isOpened = false;
            pageIter = null;
            unpinPageUsedbyIterator(currentPage);
        }
    }

}
