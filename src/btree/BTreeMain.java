package btree;

/**
 * B+Tree Lab
 * @author Michael Hay mhay@colgate.edu
 */
public class BTreeMain {

    /**
     * Write a program that demonstrates your B+Tree implementation.  This program must
     * clearly demonstrate the full functionality of your implementation.  E.g., you will
     * want to insert enough records to trigger splitting of both leaf and non-leaf nodes.
     * @param args
     */
    public static void main(String[] args) {
        BTree bTree = new BTree(2);

        //tree representations
        System.out.println("#xxx# represents one node, xx;xx seperates entries in one node, for each data entry:key,record");
        System.out.print(System.lineSeparator());

        //empty tree
        System.out.println("Origin tree with order 2(empty):");
        System.out.println(bTree);
        System.out.print(System.lineSeparator()+System.lineSeparator());

        //insert 4 records into the tree
        bTree.insert(2, "record r2");
        //bTree.insert(1,"record r1");
        bTree.insert(3, "record r3");
        //bTree.insert(4,"new record2");
        bTree.insert(5,"record r5");
        //bTree.insert(6,"record r6");
        bTree.insert(7,"record r7");
        try {
            bTree.insert(2, "record r2");
            throw new RuntimeException("should not reach here!");
        } catch (BTreeException e) {
            // expected
        }
        System.out.println("Tree with 4 data entries:");
        System.out.println(bTree);
        System.out.print(System.lineSeparator()+System.lineSeparator());

        //insert 5th record and leaf split
        bTree.insert(8,"record r8");
        System.out.println("Tree with 5 data entries and 1 index entry(1 leaf split):");
        System.out.println(bTree);
        System.out.print(System.lineSeparator()+System.lineSeparator());

        //insert 6th,7th record and leaf split again
        bTree.insert(14,"record r14");
        bTree.insert(16,"record r16");
        System.out.println("Tree with 7 data entries and 2 index entry(2 leaf splits):");
        System.out.println(bTree);
        System.out.print(System.lineSeparator()+System.lineSeparator());

        //insert 4 more records and leaf split again
        bTree.insert(19,"record r19");
        bTree.insert(20,"record r20");
        bTree.insert(22,"record r22");
        bTree.insert(24,"record r24");
        System.out.println("Tree with 11 data entries and 4 index entry(3 leaf splits):");
        System.out.println(bTree);
        System.out.print(System.lineSeparator()+System.lineSeparator());

        //insert 2 more record and innernode split
        bTree.insert(27,"record r27");
        bTree.insert(29,"record r29");
        System.out.println("Tree with 12 data entries and 5 index entry(1 innernode splits):");
        System.out.println(bTree);
        System.out.print(System.lineSeparator()+System.lineSeparator());

        //insert 4 more record and another innernode split
        bTree.insert(33,"record r33");
        bTree.insert(34,"record r34");
        bTree.insert(38,"record r38");
        bTree.insert(39,"record r39");
        System.out.println("Tree with 16 data entries and 7 index entry(2 innernode splits):");
        System.out.println(bTree);
        System.out.print(System.lineSeparator()+System.lineSeparator());

        //insert records with keys in between
        bTree.insert(4,"record r4");
        bTree.insert(9,"record r9");
        bTree.insert(10,"record r10");
        bTree.insert(23,"record r23");

        System.out.println("Tree with 20 data entries and internal inserts:");
        System.out.println(bTree);               // should print entire tree
        System.out.print(System.lineSeparator()+System.lineSeparator());

        System.out.println("Get some arbitrary recrods:");
        System.out.println(bTree.getRecord(2));  // should print 'record r2'
        System.out.println(bTree.getRecord(16));  // should print 'record r12'
        System.out.println(bTree.getRecord(24));  // should print 'record r24'
        System.out.println(bTree.getRecord(12));  // should print 'null' since there is no record with key 12 in the tree

    }
}
