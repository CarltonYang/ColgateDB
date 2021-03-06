package colgatedb.main;

import colgatedb.Catalog;
import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.operators.*;
import colgatedb.transactions.Transaction;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Op;
import colgatedb.tuple.StringField;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.Type;

import java.io.IOException;
import java.util.ArrayList;

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
public class Lab10Main {

    public static void main(String[] argv)
            throws DbException, TransactionAbortedException, IOException {

        // file named college.schema must be in colgatedb directory
        String filename = "college.schema";
        System.out.println("Loading schema from file: " + filename);
        Database.getCatalog().loadSchema(filename);

        // SQL query: INSERT INTO Students SELECT * FROM STUDENTS WHERE name="Alice"

        Transaction transaction = new Transaction();
        transaction.start();
        TransactionId tid = transaction.getId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("Students"));
        StringField alice = new StringField("alice", Type.STRING_LEN);
        Predicate p = new Predicate(1, Op.EQUALS, alice);
        Filter filterStudents = new Filter(p, scanStudents);
        Insert insert = new Insert(tid, filterStudents, Database.getCatalog().getTableId("Students2"));

        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        insert.open();
        while (insert.hasNext()) {
            Tuple tup = insert.next();
            System.out.println("\t"+tup);
        }
        insert.close();
        transaction.commit();
    }

}