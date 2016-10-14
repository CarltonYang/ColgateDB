package btree;

import javafx.beans.binding.ObjectExpression;

/**
 * B+Tree Lab
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * A DataEntry object represents an alternative 1 data entry.  Data entries
 * are stored in the leaves of B+Trees.
 */
public class DataEntry extends Entry {

    private Object Data;

    public DataEntry(Object obj,int key){
        super(key);
        this.Data=obj;
    }

    public String toString(){
        return Integer.toString(key)+ "," + Data.toString();
    }

    public void setData(Object obj){
        this.Data=obj;
    }

    public Object getData(){
        return this.Data;
    }
}
