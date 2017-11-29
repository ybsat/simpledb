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

    private int tableId;
    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.tableId = f.getAbsoluteFile().hashCode();
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (this.getId() != pid.getTableId()) return null;
        if (pid.getPageNumber() < 0 || pid.getPageNumber() >= this.numPages()) return null;
        try {
            int pageSize = BufferPool.getPageSize();
            byte[] byteStream = new byte[pageSize];
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            raf.seek(pageSize * pid.getPageNumber());
            raf.readFully(byteStream);
            raf.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), byteStream);
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        assert page instanceof HeapPage : "Write non-heap page to a heap file.";
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(BufferPool.getPageSize() * page.getId().getPageNumber());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double)f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        if (!td.equals(t.getTupleDesc())) throw new DbException("TupleDesc does not match.");
        int i = 0;
        HeapPage hp = null;
        for (i = 0; i < numPages(); i ++) {
            if (((HeapPage)(Database.getBufferPool().getPage(
                    tid, new HeapPageId(tableId, i), Permissions.READ_ONLY))).getNumEmptySlots() > 0)
                break;
        }
        if (i == numPages()) {
            synchronized(this) {
                i = numPages();
                // All files are full
                hp = new HeapPage(new HeapPageId(tableId, i), HeapPage.createEmptyPageData());
                try {
                    int pageSize = BufferPool.getPageSize();
                    byte[] byteStream = hp.getPageData();
                    RandomAccessFile raf = new RandomAccessFile(f, "rw");
                    raf.seek(pageSize * i);
                    raf.write(byteStream);
                    raf.close();
                }
                catch (IOException e) {
                    throw e;
                }
            }
        }
        hp = (HeapPage)(Database.getBufferPool().getPage(tid, new HeapPageId(tableId, i), Permissions.READ_WRITE));
        hp.insertTuple(t);
        ArrayList<Page> pList = new ArrayList<Page>();
        pList.add(hp);
        return pList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        if (tableId != t.getRecordId().getPageId().getTableId()) throw new DbException("Table Id does not match.");
        int pageno = t.getRecordId().getPageId().getPageNumber();
        if (pageno < 0 || pageno >= numPages()) throw new DbException("Page number is illegal.");
        HeapPage hp = (HeapPage)(Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE));
        hp.deleteTuple(t);
        ArrayList<Page> pList = new ArrayList<Page>();
        pList.add(hp);
        return pList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, tableId, numPages());
    }
}


