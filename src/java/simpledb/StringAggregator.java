package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public HashMap<Field, Integer> count;
    public int gbfieldIndex;
    public Type gbfieldType;
    public int afieldIndex;
    public Op op;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbfieldType=gbfieldtype;
        gbfieldIndex=gbfield;
        afieldIndex=afield;
        op=what;
        count=new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupBy;
        if (gbfieldIndex==NO_GROUPING){
            groupBy=null;
        }
        else{
            groupBy=tup.getField(gbfieldIndex);
        }
        if(!count.containsKey(groupBy)){
            count.put(groupBy,0); // initialize count to 0
        }
        if(op!=Op.COUNT){
            throw new IllegalArgumentException();
        }
        else{
            int newCount=count.get(groupBy)+1;
            count.put(groupBy,newCount);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>(); // to iterate over
        String[] fieldNames;
        Type[] fieldTypes;
        if(gbfieldIndex==NO_GROUPING){
            fieldNames=new String[] {"aggregateVal"};
            fieldTypes = new Type[] {Type.INT_TYPE};
        }
        else {
            fieldNames = new String[] {"groupValue", "aggregateValue"};
            fieldTypes = new Type[] {gbfieldType, Type.INT_TYPE};
        }
        TupleDesc td=new TupleDesc(fieldTypes, fieldNames);
        for(Field fieldName: count.keySet()){
            Tuple toAdd=new Tuple(td);
            if(gbfieldIndex==NO_GROUPING){
                toAdd.setField(0, new IntField(count.get(fieldName)));
            }
            else{
                toAdd.setField(0, fieldName);
                toAdd.setField(1, new IntField(count.get(fieldName)));
            }
            tuples.add(toAdd);
        }
        return new TupleIterator(td,tuples);
    }
}
