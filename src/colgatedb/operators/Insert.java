package colgatedb.operators;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.IntField;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

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
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private int numInsert;
    private boolean called;
    private Tuple current;
    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableid = tableid;
        setTupleDesc(child.getTupleDesc());
        if (!Database.getCatalog().getTupleDesc(tableid).equals(child.getTupleDesc())) {
            throw new DbException("TupleDesc from child and table don't match!");
        }
        called = false;
        numInsert = 0;
        current = new Tuple(getTupleDesc());
    }

    /**
     * @return tuple desc of the insert operator should be a single INT named count
     */
    @Override
    public TupleDesc getTupleDesc() {
        String[] fieldAr= new String[1];
        fieldAr[0]="count";
        return new TupleDesc(new Type[]{Type.INT_TYPE}, fieldAr);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException { }

    @Override
    public void close() {}

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        called = false;
    }

    /**
     *
     * @return true if this is the first time being called...  even if child is empty,
     *         this iterator still has one tuple to return (the tuple that says that zero
     *         records were inserted).
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return (!called);
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a single-field tuple containing the number of
     * inserted records. Inserts should be completed by calling the insertTuple
     * method on the appropriate DbFile (which can be looked up in the Catalog).
     * <p>
     * Note that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * @return A single-field tuple containing the number of inserted records.
     * @throws NoSuchElementException if called more than once
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (!hasNext()){
            throw new NoSuchElementException("Called more than once!");
        }
        else{
            called = true;
            child.open();
                while (child.hasNext()) {
                    Database.getCatalog().getDatabaseFile(tableid).insertTuple(t, child.next());
                    numInsert++;
                }
            current.setField(0, new IntField(numInsert));
            return current;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new DbException("Expected only one child!");
        }
        this.child = children[0];
    }
}

