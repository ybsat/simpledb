package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public int[] hist;
    public double bucketSize;
    public int numBuckets;
    public int minVal;
    public double numTuples;

    public IntHistogram(int buckets, int min, int max) {
    	numBuckets=buckets;
    	bucketSize=(int) Math.ceil((double) (max - min + 1)/buckets);
        hist = new int[buckets];
    	for(int i=0; i<numBuckets; i++){
    	    hist[i]=0;
        }
    	minVal=min;
    	numTuples=0;
    }

    public int findBucket(int v){
        int bucket = (v-minVal)/(int)bucketSize;
        if(bucket>=numBuckets){
            return numBuckets;
        }
        if(bucket<0){
            return -1;
        }
        return bucket;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int addBucket = (v-minVal)/(int)bucketSize;
    	hist[addBucket]=hist[addBucket]+1;
    	numTuples+=1;
    }

    public void deleteValue(int v) {
        int delBucket = (v-minVal)/(int)bucketSize;
        hist[delBucket]=hist[delBucket]-1;
        numTuples-=1;
    }

    public double lessThan(int v){
        int bucket=findBucket(v);
        double h_b, b_f, selectivity=0;
        double b_left=(bucketSize*(bucket-1))+minVal;
        if(bucket<0){
            b_left=-1;
            b_f=0;
            h_b=0;
        }
        else if(bucket>=numBuckets){
            b_left=numBuckets-1;
            b_f=0;
            h_b=0;
        }
        else{
            b_left=bucket-1;
            h_b=((double)hist[bucket]);
            b_f=(v-(b_left*bucketSize)+minVal)/bucketSize;
        }
        selectivity=(h_b*b_f)/numTuples;
        if(b_left<0){
            return selectivity/numTuples;
        }
        for(int i=0; i<bucket; i++){
            selectivity+=hist[i];
        }
        return selectivity/numTuples;
    }

    public double greaterThan(int v){
        int bucket=findBucket(v);
        double h_b, b_f, selectivity=0;
        double b_right=(bucketSize*bucket)+minVal;

        if(bucket<0){
            b_right=0;
            b_f=0;
            h_b=0;
        }
        else if(bucket>=numBuckets){
            b_right=numBuckets;
            b_f=0;
            h_b=0;
        }
        else{
            b_right=bucket+1;
            h_b=((double)hist[bucket]);
            b_f=((b_right*bucketSize)+minVal-v)/bucketSize;
        }
        selectivity=(h_b*b_f)/numTuples;
        if(b_right>=numBuckets){
            return selectivity/numTuples;
        }
        for(int i=bucket+1; i<numBuckets; i++){
            selectivity+=hist[i];
        }
        return selectivity/numTuples;
    }

    public double equals(int v){
        int bucket=findBucket(v);
        if(bucket<0 || bucket>=numBuckets){
            return 0.0;
        }
        double h_b=((double)hist[bucket]);
        return (h_b/bucketSize)/numTuples;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch(op){
            case EQUALS:
                return equals(v);
            case LESS_THAN_OR_EQ:
                return equals(v)+lessThan(v);
            case LESS_THAN:
                return lessThan(v);
            case GREATER_THAN_OR_EQ:
                return equals(v)+greaterThan(v);
            case GREATER_THAN:
                return greaterThan(v);
            case NOT_EQUALS:
                int bucket=findBucket(v);
                return 1.0-((hist[bucket]/bucketSize)/numTuples);
            default:
                return -1.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return("helo");
    }
}
