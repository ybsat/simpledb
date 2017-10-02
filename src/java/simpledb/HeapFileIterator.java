package simpledb;

import java.io.*;
import java.util.*;

/**
 * Created by ankita on 10/2/17.
 */
public class HeapFileIterator extends AbstractDbFileIterator {

    TransactionId tid;
    Iterator<Tuple> iter;
    ArrayList<Tuple> tuples;

    public HeapFileIterator(TransactionId tranid, ArrayList<Tuple> list) {
        tid = tranid;
        tuples=list;
        iter=tuples.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return iter.hasNext();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        iter=tuples.iterator();
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if(iter.hasNext()){
            return iter.next();
        }
        return null;
    }

    @Override
    public void open() {
        iter=tuples.iterator();
    }

}


