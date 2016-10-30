package colgatedb.operators;

import colgatedb.tuple.*;

import java.util.*;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afiled;
    private Op operand;
    private AggregateFields aggregateFields;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afiled = afield;
        this.operand = what;
        this.aggregateFields= new AggregateFields(operand.toString());
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(gbfieldtype==null){
            aggregateFields.incrementCount();
        }
        else{
            aggregateFields.addTuple(tup);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public DbIterator iterator(){
        aggregateFields.initializeList();
        TupleIterator tupleIterator= new TupleIterator(aggregateFields.getTupleDesc(),aggregateFields.result);
        return tupleIterator;
    }

    /**
     * A helper struct to store accumulated aggregate values.
     */
    private class AggregateFields {
        public String groupVal;
        public int count;
        public Map<Field,Integer> tempVal;
        private List<Tuple> result;

        public AggregateFields(String groupVal) {
            this.groupVal = groupVal;
            count = 0;
            this.result = new LinkedList<>();
            this.tempVal = new HashMap<>();
        }

        public void incrementCount(){
            count++;
        }

        public void addTuple(Tuple tup){
            Field gbKey = tup.getField(gbfield);
            if(tempVal.containsKey(gbKey)) {
                int tempCount  = tempVal.get(gbKey);
                switch (operand) {
                    case COUNT:
                        tempVal.put(gbKey,++tempCount);

                }
            }else{
                tempVal.put(gbKey,1);
            }
        }

        public void initializeList(){
            if (gbfieldtype!=null){
                for (Map.Entry<Field, Integer> entry : tempVal.entrySet()){
                    Tuple tuple = new Tuple(getTupleDesc());
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(entry.getValue()));
                    result.add(tuple);
                }
            }else {
                Tuple tuple = new Tuple(getTupleDesc());
                tuple.setField(0, new IntField(count));
                result.add(tuple);
            }
        }

        public TupleDesc getTupleDesc(){
            if (gbfieldtype!=null){
                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            }else {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            }
        }
    }
}
