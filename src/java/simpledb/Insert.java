package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    public TransactionId tid;
    public OpIterator feed;
    public int tableId;
    public DbFile file;
    public TupleDesc td;

    public boolean been_called;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        TupleDesc table = Database.getCatalog().getDatabaseFile(tableId).getTupleDesc();
        if(!child.getTupleDesc().equals(table)){
            throw new DbException("table td must match tuple");
        }
        tid=t;
        feed=child;
        this.tableId=tableId;
        file=Database.getCatalog().getDatabaseFile(tableId);
        Type[] type=new Type[] {Type.INT_TYPE};
        String[] names=new String[] {"Number of modified tuples"};
        td = new TupleDesc(type,names);
        been_called=false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        feed.open();
    }

    public void close() {
        feed.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        feed.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(been_called || feed==null){
            return null;
        }
        been_called = true;
        int count=0;
        while(feed.hasNext()){
            try {
                Database.getBufferPool().insertTuple(tid, tableId, feed.next());
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple tup = new Tuple(getTupleDesc());
        IntField num=new IntField(count);
        tup.setField(0,num);
        return tup;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {feed};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        feed=children[0];
    }
}
