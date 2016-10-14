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
        bTree.insert(2, "record r2");
        try {
            bTree.insert(2, "record r2");
            throw new RuntimeException("should not reach here!");
        } catch (BTreeException e) {
            // expected
        }
        bTree.insert(1,"record r1");
        bTree.insert(3,"new record");
        bTree.insert(4,"new record2");
        bTree.insert(5,"record r5");
        bTree.insert(6,"record r6");
        bTree.insert(7,"record r7");
        bTree.insert(8,"record r8");
        bTree.insert(13,"record r13");

        bTree.insert(14,"record r14");
        bTree.insert(15,"record r15");
        bTree.insert(9,"record r9");
        bTree.insert(10,"record r10");
        bTree.insert(11,"record r11");
        bTree.insert(12,"record r12");

        System.out.println(bTree.getRecord(12));  // should print 'record r2'
        System.out.println(bTree);               // should print entire tree
    }
}
