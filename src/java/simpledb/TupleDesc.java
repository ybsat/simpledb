package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    public ArrayList<TDItem> fields; //Dynamic array holding TDItem field information
    public boolean isnull; // Boolean to determine whether all field names are null or not (initialized by constructor only)
    private static final long serialVersionUID = 1L;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return this.fields.iterator();
    }


    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        fields = new ArrayList<TDItem>();
        for(int i=0; i<typeAr.length; i++) {
            TDItem curr = new TDItem(typeAr[i], fieldAr[i]);
            fields.add(curr);
        }
        isnull = false;
    }


    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * TDItem fields (field name and type slready incorporated)
     *
     * Array List<TDItem> items
     *            array with the TDItems to go into the TupleDesc
     */
    public TupleDesc(ArrayList<TDItem> items, boolean isnullmodifier){
        fields=items;
        isnull = isnullmodifier;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        fields = new ArrayList<TDItem>();
        for(int i=0; i<typeAr.length; i++) {
            TDItem curr = new TDItem(typeAr[i], null);
            fields.add(curr);
        }
        isnull = true;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.fields.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i<fields.size()){
            return fields.get(i).fieldName;
        }
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i<fields.size()){
            return fields.get(i).fieldType;
        }
        throw new NoSuchElementException();
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {

        // null search throws exception
        if (name == null) throw new NoSuchElementException();

        // if all field names are null, throws exception
        if (isnull == true) throw new NoSuchElementException();

        // find field index if name exists
        for(int i=0; i<fields.size(); i++){
           if (fields.get(i).fieldName == name){
               return i;
           }
       }

       // if field name not available, throw exception
       throw new NoSuchElementException(); //throw exception
    }


    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int sumSize=0;
        for(int i=0; i<fields.size(); i++){
            sumSize+=fields.get(i).fieldType.getLen();
        }
        return sumSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        ArrayList<TDItem> concatenated = new ArrayList<TDItem>();
        concatenated.addAll(td1.fields);
        concatenated.addAll(td2.fields);
        boolean isnullmodifier = false; // to overwrite isnull for the concatenated TD
        if (td1.isnull == true && td2.isnull == true) {isnullmodifier = true;}
        TupleDesc merged = new TupleDesc(concatenated, isnullmodifier);
        return merged;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o instanceof TupleDesc)) return false;
        TupleDesc op = (TupleDesc) o;
        if (op.fields.size() == this.fields.size()) {
            for (int i = 0; i < this.fields.size(); i++) {
                if (op.fields.get(i).fieldType != this.fields.get(i).fieldType) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {

        String str = "";
        for (int i = 0; i < fields.size() - 1; i++) {
            str += fields.toString() + ", ";
        }

        return str;
    }
}
