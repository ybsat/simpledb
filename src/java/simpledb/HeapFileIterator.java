package simpledb;

import java.io.*;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {

    TransactionId tid;
    Iterator<Tuple> iter;
    ArrayList<Tuple> tuples;

    public HeapFileIterator(TransactionId tranid, ArrayList<Tuple> list) {
        tid = tranid;
        tuples=list;
    }
    
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(iter==null){
            return false;
        }
        return iter.hasNext();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        iter=tuples.iterator();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
        if(iter==null){
            throw new NoSuchElementException();
        }
        return iter.next();
    }

    @Override
    public void open() {
        iter=tuples.iterator();
    }

    @Override
    public void close() {
        iter = null;
    }

}


