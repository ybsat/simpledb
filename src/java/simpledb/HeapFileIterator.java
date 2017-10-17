package simpledb;

import java.io.*;
import java.util.*;

public class HeapFileIterator extends AbstractDbFileIterator {

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


    public Tuple readNext() throws DbException, TransactionAbortedException, NoSuchElementException{
        if(pageIter==null){
            return null;
        }
        if(!isOpen){
            throw new NoSuchElementException();
        }

        if(pageIter.hasNext()){
            return pageIter.next();
        }

        else{
            while(currPage<numPages){
                HeapPageId pid = new HeapPageId(fileId, currPage);
                try {
                    HeapPage curr = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    pageIter=curr.iterator();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                currPage++;
                if(pageIter.hasNext()){ //found tuple
                    return pageIter.next();
                }
            }
            //throw new NoSuchElementException();
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


