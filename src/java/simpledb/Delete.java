package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    public TransactionId tid;
    public OpIterator feed;
    public TupleDesc td;
    public boolean been_called;


    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        tid=t;
        feed=child;
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
        super.close();
        feed.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        feed.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(been_called || feed==null){
            return null;
        }
        int count=0;
        been_called=true;
        while(feed.hasNext()){
            try {
                Database.getBufferPool().deleteTuple(tid, feed.next());
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
