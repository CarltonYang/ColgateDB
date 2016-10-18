package btree;

/**
 * B+Tree Lab
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * An IndexEntry object represents an entry in a non-leaf node of a B+Tree.
 */
public class IndexEntry extends Entry {

    private BTree.Node Data;

    public IndexEntry(BTree.Node tempnode, int key){
        super(key);
        this.Data=tempnode;
    }

    public void setData(BTree.Node tempnode){
        this.Data=tempnode;
    }

    public String toString() {
        if (key==Integer.MIN_VALUE){
            return "";
        }
        return Integer.toString(key);
    }

    public BTree.Node getData(){
        return this.Data;
    }
}
