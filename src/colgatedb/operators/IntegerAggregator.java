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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op operand;
    private AggregateFields aggregateFields;
    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.operand = what;
        this.aggregateFields=new AggregateFields(operand.toString());
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(gbfieldtype==null){
            aggregateFields.incrementCount(tup);
        }
        else{
            aggregateFields.addTuple(tup);
        }
    }


    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        aggregateFields.initializeList();
        TupleIterator tupleIterator= new TupleIterator(aggregateFields.getTupleDesc(),aggregateFields.result);
        return tupleIterator;
    }

    /**
     * A helper struct to store accumulated aggregate values.
     */
    private class AggregateFields {
        public String groupVal;
        public int min, max, sum, count, sumCount;
        public Map<Field,Integer> tempVal;
        public Map<Field,Integer> tempCount;
        private List<Tuple> result;

        public AggregateFields(String groupVal) {
            this.groupVal = groupVal;
            min = Integer.MAX_VALUE;
            max = Integer.MIN_VALUE;
            sum = count = sumCount = 0;
            this.result = new LinkedList<>();
            this.tempVal = new HashMap<>();
            this.tempCount = new HashMap<>();
        }

        private int getValue(Tuple tup){
            IntField field = (IntField) tup.getField(afield);
            int value = field.getValue();
            return value;
        }

        public void incrementCount(Tuple tup){
            int value= getValue(tup);
            switch (operand){
                case MIN:
                    min = Math.min(value, min);
                    break;
                case MAX:
                    max = Math.max(value, max);
                    break;
                case SUM:
                    sum += value;
                    break;
                case COUNT:
                    count++;
                    break;
                case AVG:
                    sum++;
                    sumCount += value;
            }
        }

        public void addTuple(Tuple tup) {
            int value = getValue(tup);
            Field gbKey = tup.getField(gbfield);
            if (tempVal.containsKey(gbKey)) {
                int valtemp = tempVal.get(gbKey);
                switch (operand) {
                    case MIN:
                        tempVal.put(gbKey, Math.min(valtemp, value));
                        break;
                    case MAX:
                        tempVal.put(gbKey, Math.max(valtemp, value));
                        break;
                    case SUM:
                        tempVal.put(gbKey, valtemp + value);
                        break;
                    case COUNT:
                        tempVal.put(gbKey, ++valtemp);
                        break;
                    case AVG:
                        tempVal.put(gbKey, valtemp + value);
                        int countTemp = tempCount.get(gbKey);
                        tempCount.put(gbKey, ++countTemp);
                }
            } else {
                switch (operand) {
                    case COUNT:
                        tempVal.put(gbKey, 1);
                        break;
                    case AVG:
                        tempVal.put(gbKey, value);
                        tempCount.put(gbKey, 1);
                        break;
                    default:
                        tempVal.put(gbKey, value);
                }
            }
        }

        public void initializeList(){
            if (gbfieldtype!=null){
                for (Map.Entry<Field, Integer> entry : tempVal.entrySet()) {
                    Tuple tuple = new Tuple(getTupleDesc());
                    tuple.setField(0, entry.getKey());
                    switch (operand) {
                        case AVG:
                            int average= entry.getValue()/tempCount.get(entry.getKey());
                            tuple.setField(1, new IntField(average));
                        break;
                        default:
                            tuple.setField(1, new IntField(entry.getValue()));
                        }
                    result.add(tuple);
                }
            }else {
                Tuple tuple = new Tuple(getTupleDesc());
                int tempresult=0;
                switch (operand) {
                    case MIN:
                        tempresult = min;
                        break;
                    case MAX:
                        tempresult = max;
                        break;
                    case SUM:
                        tempresult = sum;
                        break;
                    case COUNT:
                        tempresult = count;
                        break;
                    case AVG:
                        tempresult = sum/sumCount;
                }
                tuple.setField(0, new IntField(tempresult));
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
