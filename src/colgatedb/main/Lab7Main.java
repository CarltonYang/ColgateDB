package colgatedb.main;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.operators.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.*;

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
public class Lab7Main {

    public static void main(String[] argv)
            throws DbException, TransactionAbortedException, IOException {

        // file named college.schema must be in colgatedb directory
        String filename = "college.schema";
        System.out.println("Loading schema from file: " + filename);
        Database.getCatalog().loadSchema(filename);



        // initiate all three tables
        TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("Students"));
        SeqScan scanTakes = new SeqScan(tid, Database.getCatalog().getTableId("Takes"));
        SeqScan scanProfs = new SeqScan(tid, Database.getCatalog().getTableId("Profs"));

        // select professor hay from all profs table
        StringField hay = new StringField("hay", Type.STRING_LEN);
        Predicate p = new Predicate(1, Op.EQUALS, hay);
        Filter filterProfs = new Filter(p, scanProfs);

        // join professor hay's favourite course with Takes table
        JoinPredicate jpc= new JoinPredicate(1, Op.EQUALS, 2);
        Join joinProfCour = new Join (jpc, scanTakes, filterProfs );

        // join previous table with Students table
        JoinPredicate jp= new JoinPredicate(0, Op.EQUALS, 0);
        Join joinStuTakes = new Join(jp, scanStudents, joinProfCour);

        // project name of the previous table
        ArrayList<Integer> fieldList= new ArrayList<Integer>(2);
        fieldList.add(0,1);
        Type[] types=new Type[1];
        types[0]=Type.STRING_TYPE;
        Project proResult = new Project( fieldList, types, joinStuTakes);

        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        proResult.open();
        while (proResult.hasNext()) {
            Tuple tup = proResult.next();
            System.out.println("\t"+tup);
        }
        proResult.close();


    }

}