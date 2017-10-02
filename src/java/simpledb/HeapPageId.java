package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     *
     */

    public int hashNum;
    public int tableNum; //this is tableId
    public int pageNum;

    public HeapPageId(int tableId, int pgNo) {
        pageNum = pgNo;
        tableNum = tableId;
        hashNum = hashCode();
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return tableNum;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        return pageNum;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // table num page num [ex: tablennum = 987, pagenum=4565, hash = 9874565]
        //double pageDigits=(java.lang.Math.log10(pageNum)+1);
        //int hash=(int)(tableNum*java.lang.Math.pow(10.0,pageDigits)) + pageNum;
        //return hash;

        return java.util.Objects.hash(tableNum, pageNum);
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if(o instanceof HeapPageId && o != null){
            if ((((HeapPageId) o).pageNum==this.pageNum) && (((HeapPageId) o).tableNum==this.tableNum)){
                return true;
            }
        }
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
