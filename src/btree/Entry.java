package btree;

/**
 * B+Tree Lab
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * Abstract parent class for both IndexEntry and DataEntry.  All entries
 * have a key, which should be initialized upon construction.
 */
public abstract class Entry {

    protected int key;  // initialize in constructor

    public Entry(int key){
        this.key=key;
    }

    abstract public String toString();

    public int key() {
        return key;
    };
}
