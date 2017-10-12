package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */

    private int field1Num;
    private int field2Num;
    private Predicate.Op operator;

    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        field1Num = field1;
        field2Num = field2;
        operator = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        Field field1 = t1.getField(field1Num);
        Field field2 = t2.getField(field2Num);
        return field1.compare(operator, field2);
    }
    
    public int getField1()
    {
        return field1Num;
    }
    
    public int getField2()
    {
        return field2Num;
    }
    
    public Predicate.Op getOperator()
    {
        return operator;
    }
}
