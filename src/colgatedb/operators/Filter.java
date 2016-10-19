package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

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
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate p;
    private DbIterator child;
    private boolean open;
    private boolean work;
    private Tuple current;
    //private TupleDesc td;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p=p;
        this.child=child;
        this.setTupleDesc(child.getTupleDesc());
        open=false;
        work=true;
        current=null;
    }

    public Predicate getPredicate() {
        return p;
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        open=true;
    }

    @Override
    public void close() {
        child.close();
        open=false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!open){
            return false;
        }
        else if (!work){
            return true;
        }
        else{
            while (child.hasNext()){
                Tuple temp=child.next();
                if (p.filter(temp)){
                    current=temp;
                    work=false;
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more tuples!");
        }
        work=true;
        return current;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new DbException("Expected only one child!");
        }
        child = children[0];
    }

}
