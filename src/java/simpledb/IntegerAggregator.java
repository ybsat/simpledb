package simpledb;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    public int gbfield; //group by field
    public Type gbfieldtype; //group by type
    public Op op; //operator
    public int afield; // field to agg over
    public Tuple agg; // holds agg val
    public Tuple group; // holds group by val

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
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        op=what;
        this.afield=afield;

        TupleDesc aggTup = new TupleDesc(int,"agg");
        agg=new Tuple(aggTup);

        TupleDesc groupTup = new TupleDesc(gbfieldtype, "group");
        group=new Tuple(groupTup);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // find a field
        // assign to group by
        // perform op on it


        // some code goes here
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
        // some code goes here
        // iterate through the 2 tuples
        throw new
        UnsupportedOperationException("please implement me for lab2");
    }

}
