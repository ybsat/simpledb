package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
//import java.util.Set;
//import java.util.HashSet;

/**
 * Created by ankita on 11/20/17.
 */
public class LockState {
    public HashMap<PageId, ArrayList<TransactionId>> sharedLockTable;
    public HashMap<PageId, TransactionId> xLockTable;
    public HashMap<TransactionId, ArrayList<PageId>> sharedTransPages;
    public HashMap<TransactionId, ArrayList<PageId>> xTransPages;

    public LockState(){
        sharedLockTable= new HashMap<PageId, ArrayList<TransactionId>>();
        xLockTable=new HashMap<PageId, TransactionId>();
        sharedTransPages= new HashMap<TransactionId, ArrayList<PageId>>();
        xTransPages=new HashMap<TransactionId, ArrayList<PageId>>();
    }


    public boolean addLock(PageId pid, Permissions perm, TransactionId tid){
        if(perm==Permissions.READ_ONLY) {
            TransactionId exc = xLockTable.get(pid);
            ArrayList<TransactionId> tidShared = sharedLockTable.get(pid);
            // if there is no exclusive lock or this tid has an exclusive lock, allow the lock
            if (exc == null || exc.equals(tid)) {
                // if no tids in list yet, create arraylist
                if (tidShared == null) {
                    tidShared = new ArrayList<TransactionId>();
                }
                tidShared.add(tid);
                sharedLockTable.put(pid, tidShared);
                ArrayList<PageId> sharedPages = sharedTransPages.get(tid);
                if (sharedPages == null) {
                    sharedPages = new ArrayList<PageId>();
                }
                sharedPages.add(pid);
                sharedTransPages.put(tid, sharedPages);
                return true;
            }
        }
        else if(perm==Permissions.READ_WRITE){
            TransactionId exc = xLockTable.get(pid);
            ArrayList<TransactionId> shared = sharedLockTable.get(pid);
            if((shared != null && shared.size() > 1)
                    || (shared != null && shared.size() == 1 && !shared.contains(tid))
                    || (exc != null && !exc.equals(tid))){
                return false;
            }
            else {
                xLockTable.put(pid,tid);
                ArrayList<PageId> xPages = xTransPages.get(tid);
                if(xPages==null){
                    xPages=new ArrayList<PageId>();
                }
                xPages.add(pid);
                xTransPages.put(tid,xPages);
                return true;
            }
        }
        return false;
    }

    public void releaseSingleLock(TransactionId tid, PageId pid){
        // check in shared locks
        if(sharedTransPages.get(tid)!=null){
            sharedTransPages.get(tid).remove(pid);
        }
        if(sharedLockTable.get(pid)!=null){
            sharedLockTable.get(pid).remove(tid);
        }
        if(xTransPages.get(tid)!=null){
            xTransPages.get(tid).remove(pid);
        }
        xLockTable.remove(pid);
    }

    public boolean holdsLock(TransactionId tid, PageId pid){
        //check shared
        if(sharedLockTable.get(pid)!=null && sharedLockTable.get(pid).contains(tid)){
            return true;
        }
        // check exclusive
        else if(xLockTable.get(pid)!=null && xLockTable.get(pid).equals(tid)){
            return true;
        }
        else{
            return false;
        }
    }

    public void releaseAllLocks(TransactionId tid){
        ArrayList<PageId> sharedPages = sharedTransPages.get(tid);
        for(PageId x: sharedPages){
            sharedLockTable.get(x).remove(tid);
        }
        sharedTransPages.remove(tid);
        ArrayList<PageId> xPages = xTransPages.get(tid);
        for(PageId x: xPages){
            xLockTable.remove(x);
        }
        xTransPages.remove(tid);
    }

}
