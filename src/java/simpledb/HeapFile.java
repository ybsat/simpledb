package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc schema;
    private int fileid;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        schema = td;
        fileid = file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return fileid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return schema;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if (pid.getTableId() != fileid) {
            throw new IllegalArgumentException();
        }
        int pageNum = pid.getPageNumber();
        int pageSize = Database.getBufferPool().getPageSize();


        try {
            int offset = pageSize * pageNum;
            byte[] bytesToRead = new byte[pageSize];
            RandomAccessFile raf = new RandomAccessFile(file.getAbsolutePath(), "rw");
            raf.seek(offset);
            raf.read(bytesToRead);
            raf.close();
            HeapPageId hpid;
            hpid = (HeapPageId) pid;
            HeapPage pageRead = new HeapPage(hpid, bytesToRead);
            return pageRead;
        } catch (IOException ex) {
            try {
                throw new IOException(ex.getMessage());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();
        int pageNum = pid.getPageNumber();
        int pageSize = Database.getBufferPool().getPageSize();
        try {
            int offset = pageSize * pageNum;
            byte[] bytesToWrite = page.getPageData();
            RandomAccessFile raf = new RandomAccessFile(file.getAbsolutePath(), "rw");
            raf.seek(offset);
            raf.write(bytesToWrite);
            raf.close();
        } catch (IOException ex) {
            try {
                throw new IOException(ex.getMessage());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int pagesize = Database.getBufferPool().getPageSize();
        int filesize = (int) file.length();

        int numPages = filesize / pagesize;

        if (numPages * pagesize < filesize) numPages++;

        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapPageId id;
        ArrayList<Page> modified = new ArrayList<Page>();
        boolean found=false;
        for(int i=0; i<numPages(); i++){
            id=new HeapPageId(fileid, i);
            HeapPage curr = (HeapPage) Database.getBufferPool().getPage(tid, id, Permissions.READ_WRITE);
            if(curr.getNumEmptySlots() > 0){
                curr.insertTuple(t);
                found=true;
                modified.add(curr);
                break;
            }
        }
        if(!found){
            id=new HeapPageId(fileid, numPages());
            HeapPage toAdd = new HeapPage(id, HeapPage.createEmptyPageData());
            toAdd.insertTuple(t);
            writePage(toAdd);
            modified.add(toAdd);
        }
        return modified;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        if((t.getRecordId()==null)||(getId() != t.getRecordId().getPageId().getTableId())) {
            throw new DbException("no tuple found");
        }
        RecordId rec=t.getRecordId();
        HeapPageId pageId = (HeapPageId) rec.getPageId();
        HeapPage page = null;
        try {
            page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        page.deleteTuple(t);
        ArrayList<Page> modified = new ArrayList<Page>();
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
//        int pageCount = numPages();
//        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
//        for (int i = 0; i < pageCount; i++) {
//            PageId hpid = new HeapPageId(fileid, i);
//            try {
//                HeapPage curr = null;
//                try {
//                    curr = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                Iterator<Tuple> iter = curr.iterator();
//                while (iter.hasNext()) {
//                    tuples.add(iter.next());
//                }
//            } catch (TransactionAbortedException e) {
//                e.printStackTrace();
//            } catch (DbException e) {
//                e.printStackTrace();
//            }
//        }
        return new HeapFileIterator(tid, fileid, numPages());
    }
}


