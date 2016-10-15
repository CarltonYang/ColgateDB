package btree;

import java.util.*;

/**
 * B+Tree Lab
 * @author Michael Hay mhay@colgate.edu
 */
public class BTree {

    private static int degree;
    private static Node root;
    private HashSet<Integer> records;

    /**
     * Create a B+Tree having the specified order.
     * @param order the order of the tree (the d parameter in lecture and in the book).
     */
    public BTree(int order) {
        degree=order;
        root= new LeafNode();
        records= new HashSet<Integer>();
    }

    /**
     * Inserts a record with the given key.
     * @param key search key value of record
     * @param record record to insert
     * @throws BTreeException if this key already contained in tree
     */
    public void insert(int key, Object record) {
        if (!records.contains(key)){
            records.add(key);
            DataEntry temp=new DataEntry(record,key);
            IndexEntry newchildentry=null;
            insert_entry(root,temp,newchildentry);
        }
        else{
            throw new BTreeException("This key is already in the Btree!");
        }
    }

    private IndexEntry insert_entry(Node pointer, DataEntry dataEntry, IndexEntry newchildentry){
        if (!(pointer instanceof LeafNode)){
            int temp=0;
            boolean inserted=false;
            int vecsize= pointer.getEntry().size();
                for (int i=0; i< vecsize-1;i++){
                    int k=pointer.getEntry().get(i).key();
                    int k1=pointer.getEntry().get(i+1).key();
                    if (dataEntry.key()>=k && dataEntry.key()<k1){
                        temp=i;
                        inserted=true;
                        break;
                    }
                }
            if (!inserted){
                temp=vecsize-1;
            }
            newchildentry=insert_entry(((IndexEntry)((InnerNode) pointer).getEntry().get(temp)).getData(),dataEntry,newchildentry);
            if (newchildentry==null){
                return newchildentry;
            }
            else{
                if (pointer.size()<2*degree+1){
                    newchildentry.getData().setParent(pointer);
                    ((InnerNode) pointer).insert(newchildentry);
                    newchildentry=null;
                    return newchildentry;
                }
                else{
                    newchildentry.getData().setParent(pointer);
                    ((InnerNode)pointer).insert(newchildentry);
                    InnerNode innerNode2=new InnerNode();
                    int i=pointer.size();
                    while (i>degree){
                        IndexEntry tempidx= ((InnerNode)pointer).deleteEntry(degree);
                        innerNode2.insert(tempidx);
                        i--;
                    }
                    newchildentry=new IndexEntry(innerNode2,innerNode2.getEntry().get(0).key());
                    insertNewRoot(pointer,innerNode2,newchildentry);
                    return newchildentry;
                }
            }
        }

        else if (pointer instanceof LeafNode){
            if (pointer.size()<(2*degree)){
                ((LeafNode)pointer).insert(dataEntry);
                newchildentry=null;
                return newchildentry;
            }
            else{
                ((LeafNode)pointer).insert(dataEntry);
                LeafNode leafNode2=new LeafNode();
                int i=pointer.size();
                while (i>degree){
                    DataEntry temp= ((LeafNode)pointer).deleteEntry(degree);
                    leafNode2.insert(temp);
                    i--;
                }
                newchildentry=new IndexEntry(leafNode2,leafNode2.getEntry().get(0).key());
                insertNewRoot(pointer,leafNode2,newchildentry);
                return newchildentry;
            }
        }
        return null;
    }

    private void insertNewRoot(Node pointer,Node otherPointer,IndexEntry newchildentry){
        if (pointer.getParent()==null){
            Node newroot = new InnerNode();
            pointer.setParent(newroot);
            otherPointer.setParent(newroot);
            newroot.insert(new IndexEntry(pointer,Integer.MIN_VALUE));
            newroot.insert(newchildentry);
            this.root=newroot;
        }
    }

    /**
     * Searches for record with given key value.
     * @param key search key
     * @return Record associated with given search key value or null if no such record exists.
     */
    public Object getRecord(int key) {
        LeafNode temp= (LeafNode)tree_search(root,key);
        if (temp!=null){
            return temp.search(key);
        } else{
            return null;
        }
    }

    private Node tree_search(Node pointer, int key){
        if (pointer instanceof LeafNode){
            return pointer;
        } else {
            InnerNode pointertemp= (InnerNode) pointer;
            if (key<pointer.getEntry().get(1).key()){
                return tree_search(((IndexEntry)pointertemp.getEntry().get(0)).getData(),key);
            }
            else{
                int vecsize= pointer.getEntry().size();
                if (key>=pointer.getEntry().get(vecsize-1).key()){
                    return tree_search(((IndexEntry)pointertemp.getEntry().get(vecsize-1)).getData(),key);
                }
                else{
                    for (int i=1;i<vecsize-1;i++){
                        int k=pointer.getEntry().get(i).key();
                        int k1=pointer.getEntry().get(i+1).key();
                        if (k<=key && key<k1){
                            return tree_search(((IndexEntry)pointertemp.getEntry().get(i)).getData(),key);
                        }
                    }
                }
            }
        }
        return null;
    }
    /**
     * Returns a nicely formatted string representation of the entire tree.  The string representation
     * should be clear enough that someone could look at it and simulate search and/or insertion of
     * new entries.
     * @return string representation of the tree
     */
    @Override
    public String toString() {
        Vector<Node> printList = new Vector();
        printList.add(root);
        String temp = "";
        boolean finished = false;
        while(! finished) {
            Vector<Node> nextLevelList = new Vector();
            for(int i=0; i < printList.size(); i++) {
                Node node = (Node)printList.elementAt(i);
                temp += node.toString() + " ";
                if(node instanceof LeafNode) {
                    finished = true;
                } else {
                    for(int j=0; j < node.size() ; j++) {
                        nextLevelList.add(((InnerNode)node).getPointerAt(j));
                    }
                }
            }
            temp+=System.lineSeparator();
            printList = nextLevelList;
        }
        return temp;
    }


    /**
     * Internal tree node.
     * @see IndexEntry
     */
    public static class Node {
        protected Node parent;
        protected Vector<Entry> entry;

        Node() {
            parent = null;
            entry = new Vector();
        }

        //to be overriden in subclasses
        public void insert(IndexEntry indexEntry){
        }

        //to be overriden in subclasses
        public String toString() {
            return null;
        }

        public Node getParent(){
            return this.parent;
        }

        public Vector<Entry> getEntry(){
            return entry;
        }

        public int size(){
            return entry.size();
        }

        public void setParent(Node parent) {
            this.parent = parent;
        }


    }

    public class InnerNode extends Node{

        public InnerNode() {
            super();
        }

        public Node getPointerAt(int index) {
            return ((IndexEntry)entry.get(index)).getData();
        }

        public IndexEntry deleteEntry(int i){
            IndexEntry temp= (IndexEntry) entry.get(i);
            entry.remove(i);
            return temp;
        }

        public void insert(IndexEntry indexEntry){
            int key=indexEntry.key();
            int i=0;
            while (i< entry.size()){
                if (entry.get(i).key()>key){
                    break;
                }
                i++;
            }
            entry.add(i,indexEntry);
        }

        public String toString() {
            String s = "#";
            for(int i=1; i < entry.size(); i++) {
                s += ((Entry)entry.get(i)).toString() + " ";
            }
            s=s.substring(0,s.length()-1);
            return s + "#";
        }
    }

    public class LeafNode extends Node{

        LeafNode() {
            super();
        }

        public void insert(DataEntry dataEntry){
            int key=dataEntry.key();
            int i=0;
            while (i< entry.size()){
                if (entry.get(i).key()>key){
                    break;
                }
                i++;
            }
            entry.add(i,dataEntry);
        }

        public DataEntry deleteEntry(int i){
            DataEntry temp= (DataEntry) entry.get(i);
            entry.remove(i);
            return temp;
        }

        public Object search(int key) {
            for(int i=0; i < entry.size(); i++) {
                if( (entry.get(i)).key() == key ) {
                    return ((DataEntry) entry.get(i)).getData();
                }
            }
            return null;
        }

        public String toString() {
            String s = "#";
            for(int i=0; i < entry.size(); i++) {
                s += ((Entry)entry.get(i)).toString() + ";";
            }
            s=s.substring(0,s.length()-1);
            return s + "#";
        }
    }
}
