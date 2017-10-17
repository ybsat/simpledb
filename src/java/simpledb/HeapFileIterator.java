package simpledb;

import java.io.*;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {

    TransactionId tid;
    Iterator<Tuple> pageIter;
    int numPages;
    int fileId;
    int currPage;
    boolean isOpen;

    public HeapFileIterator(TransactionId tranid, int fileid, int numPages) {
        tid = tranid;
        this.fileId=fileid;
        this.numPages=numPages;
        currPage=0;
        pageIter=null;
        isOpen=false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {

        if (pageIter == null) {
            return false;
        }
        if (pageIter.hasNext()) {
            return true;
        } else {
            while (currPage < numPages) {
                HeapPageId pid = new HeapPageId(fileId, currPage);
                try {
                    HeapPage curr = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    pageIter = curr.iterator();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                currPage++;
                if (pageIter.hasNext()) { //found tuple
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {

        if (pageIter == null || !isOpen) {
            throw new NoSuchElementException();
        }
//        if (!isOpen) {
//            throw new NoSuchElementException();
//        }

        if (pageIter.hasNext()) {
            return pageIter.next();
        } else {
            while (currPage < numPages) {
                HeapPageId pid = new HeapPageId(fileId, currPage);
                try {
                    HeapPage curr = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    pageIter = curr.iterator();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                currPage++;
                if (pageIter.hasNext()) { //found tuple
                    return pageIter.next();
                }
            }
        }
        return null;
    }

    @Override
    public void open() {
        isOpen=true;
        currPage=0;
        pageIter=null;
        HeapPageId pid = new HeapPageId(fileId, currPage);
        try {
            HeapPage curr = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            pageIter=curr.iterator();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }
        currPage++;
    }

    @Override
    public void close() {
        isOpen=false;
        currPage=0;
        pageIter=null;
    }

}
