package simpledb;

import com.sun.org.apache.bcel.internal.generic.LCONST;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;

/**
 * Created by ankita on 11/20/17.
 */
public class LockState {
    public class LockObj{
        public PageId pid;
        public Permissions perm;
        public TransactionId tid;
        public LockObj(PageId pid, Permissions perm, TransactionId tid){
            this.pid=pid;
            this.perm=perm;
            this.tid=tid;
        }
    }
    public HashMap<TransactionId, ArrayList<PageId>> transactions;
    public HashMap<PageId, HashMap<TransactionId,LockObj>> lockTable;
    public LinkedList<LockObj> lockQ;
    public LockState(){
        this.lockTable=new HashMap<PageId, HashMap<TransactionId, LockObj>>();
        this.lockQ=new LinkedList<LockObj>();
    }

    public void addToTransactions(TransactionId tid, PageId pid){
        if(transactions.get(tid)==null) {
            ArrayList<PageId> tempArr = new ArrayList<PageId>();
            transactions.put(tid, tempArr);
        }
        transactions.get(tid).add(pid);
    }
    public void addLock(PageId pid, Permissions perm, TransactionId tid){
        LockObj lock = new LockObj(pid, perm, tid);
        if(lockTable.containsKey(pid)){
            if(lockTable.get(pid).size()==1){
                // either exclusive or only one shared
                for(TransactionId x: lockTable.get(pid).keySet()) {
                    if(lockTable.get(pid).get(x).perm.equals(Permissions.READ_WRITE) || lock.perm==Permissions.READ_WRITE){
                        lockQ.add(lock);
                        break;
                    }
                    else{
                        lockTable.get(pid).put(tid,lock);
                        addToTransactions(tid,pid);
                    }
                }
            }
            else {
                // multiple locks (shared lock)
                if(lock.perm==Permissions.READ_ONLY){
                    lockTable.get(pid).put(tid,lock);
                    addToTransactions(tid,pid);
                }
                else{
                    lockQ.add(lock);
                }
            }
        }
        else {
            HashMap<TransactionId, LockObj> tempHM = new HashMap<TransactionId, LockObj>();
            tempHM.put(tid,lock);
            lockTable.put(pid,tempHM);
            addToTransactions(tid,pid);
        }
    }

    public void removeSingleLock(TransactionId tid, PageId pid){
        transactions.get(tid).remove(pid);
        lockTable.get(pid).remove(tid);
        if(transactions.get(tid).size()==0){
            transactions.remove(tid);
        }
        if(lockTable.get(pid).size()==0){
            lockTable.remove(pid);
        }
    }

    public void removeAllLocks(TransactionId tid){
        if(transactions.get(tid)==null){
            return;
        }
        ArrayList<PageId> pagesToRemove = transactions.get(tid);
        transactions.remove(tid);
        for(PageId pid: pagesToRemove){
            lockTable.get(pid).remove(tid);
            if(lockTable.get(pid).size()==0){
                lockTable.remove(pid);
            }
        }
    }

    public void updateQueue(){
        boolean feasible=true;
        while(feasible){
            if(lockQ.isEmpty()){
                feasible=false;
                break;
            }
            LockObj curr = lockQ.element();

            // if nothing has lock on page
            if(!lockTable.containsKey(curr.pid)){
                HashMap<TransactionId, LockObj> tempHM = new HashMap<TransactionId, LockObj>();
                tempHM.put(curr.tid,curr);
                lockTable.put(curr.pid,tempHM);
                addToTransactions(curr.tid,curr.pid);
                lockQ.remove();
            }

            // if it is read_only
            else if(curr.perm==Permissions.READ_ONLY){
                if(lockTable.get(curr.pid).size()==1){
                    for(TransactionId x: lockTable.get(curr.pid).keySet()) {
                        // if exclusive
                        if(lockTable.get(curr.pid).get(x).perm.equals(Permissions.READ_WRITE)){
                            feasible=false;
                            break;
                        }
                    }
                }
                // not 1 obj, all shared already
                lockTable.get(curr.pid).put(curr.tid,curr);
                lockQ.remove();
                addToTransactions(curr.tid, curr.pid);
            }
            else{ //for read write where pid locktable is not empty
                feasible=false;
            }
        }
    }
}
