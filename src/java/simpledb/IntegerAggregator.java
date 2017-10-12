package simpledb;


import java.util.HashMap;
import java.util.ArrayList;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    public int gbfieldIndex; //group by field
    public Type gbfieldType;
    public Op op; //operator
    public int afieldIndex; // field to agg over
    public HashMap<Field, Integer> agg; // to hold agg values
    public HashMap<Field, Integer> count; // to hold count for use in averaging


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbfieldIndex=gbfield;
        gbfieldType=gbfieldtype;
        op=what;
        afieldIndex=afield;
        agg= new HashMap<Field, Integer>();
        count=new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbField;
        if(gbfieldIndex==NO_GROUPING){
            gbField=null;
        }
        else{
            gbField=tup.getField(gbfieldIndex);
        }

        if(!agg.containsKey(gbField)){
            count.put(gbField,0);
            int initial=0;
            switch(op){
                case MIN:
                    initial=Integer.MAX_VALUE;
                    break;
                case MAX:
                    initial = Integer.MIN_VALUE;
                    break;
            }
            agg.put(gbField,initial);
        }

        int valToAdd = ((IntField)(tup.getField(afieldIndex))).getValue();
        int currVal = agg.get(gbField);
        int newVal=currVal;

        switch(op) {
            case MIN:
                if (valToAdd < currVal) {
                    newVal = valToAdd;
                }
                break;
            case MAX:
                if (valToAdd > currVal) {
                    newVal = valToAdd;
                }
                break;
            case AVG: case SUM:case COUNT:
                int c = count.get(gbField); // to keep track of count for averaging/count at the end
                newVal = currVal + valToAdd;
                count.put(gbField, c + 1);
                break;
        }
        agg.put(gbField,newVal);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>(); // to iterate over
        String[] fieldNames;
        Type[] fieldTypes;
        if (gbfieldIndex == Aggregator.NO_GROUPING) {
            fieldNames = new String[] {"aggregateVal"};
            fieldTypes = new Type[] {Type.INT_TYPE};
        }
        else {
            fieldNames = new String[] {"groupValue", "aggregateValue"};
            fieldTypes = new Type[] {gbfieldType, Type.INT_TYPE};
        }
        TupleDesc td = new TupleDesc(fieldTypes, fieldNames);

        // create tuple for every field to be grouped over
        for(Field fieldName: agg.keySet()){
            int aggVal;
            Tuple toAdd;
            if (op==Op.AVG){
                aggVal=agg.get(fieldName) / count.get(fieldName);
            }
            else if(op==Op.COUNT){
                aggVal=count.get(fieldName);
            }
            else{
                aggVal=agg.get(fieldName);
            }
            toAdd = new Tuple(td);
            if(gbfieldIndex==NO_GROUPING){
                toAdd.setField(0, new IntField(aggVal));
            }
            else{
                toAdd.setField(0, fieldName);
                toAdd.setField(1, new IntField(aggVal));
            }
            tuples.add(toAdd);
        }
        return new TupleIterator(td, tuples);
    }

}
