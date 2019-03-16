package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 */
public class LockManager {
    // These members are given as a suggestion. You are not required to use them, and may
    // delete them and add members as you see fit.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    private Map<ResourceName, List<Pair<Long, Lock>>> resourceLocks = new HashMap<>();
    private Deque<LockRequest> waitingQueue = new ArrayDeque<>();

    // You should not modify this.
    protected Map<Object, LockContext> contexts = new HashMap<>();

    public LockManager() {}

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public LockContext databaseContext() {
        if (!contexts.containsKey("database")) {
            contexts.put("database", new LockContext(this, null, "database"));
        }
        return contexts.get("database");
    }

    /**
     * Create a lock context with no parent. Cannot be called "database".
     */
    public LockContext orphanContext(Object name) {
        if (name.equals("database")) {
            throw new IllegalArgumentException("cannot create orphan context named 'database'");
        }
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock. No error checking is performed for holding
     * requisite parent locks or freeing dependent child locks. Blocks the transaction and
     * places it in queue if the requested lock is not compatible with another transaction's
     * lock on the resource. Unblocks and unqueues all transactions that can be unblocked
     * after releasing locks in RELEASELOCKS, in order of lock request.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(BaseTransaction transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
    	
    	List<Pair<Long, Lock>> existedLocks =  resourceLocks.get(name);
    	Long tNum = transaction.getTransNum();
    	boolean comp  = true;
    	boolean release  = false;
    	boolean find  = false;
    	boolean newcomp = true;
    	Lock newLock = new Lock(name, lockType);
    	
    	//acquire
    	if (existedLocks == null) {
    		List<Pair<Long, Lock>> newlist= new ArrayList<>();
        	Pair<Long, Lock> newpair = new Pair<>(tNum, newLock);
        	newlist.add(newpair);
        	resourceLocks.put(name, newlist);
	    	for (ResourceName oldName:releaseLocks) {
	    		if (resourceLocks.containsKey(oldName) == false) {
	    			release = false;
	    			throw new NoLockHeldException("NoLockHeldException");
	    		}else {
	    		resourceLocks.remove(oldName);
	    		release = true;}
	    	}
    	}
    	else {
	    	
    	for (Pair<Long, Lock> existedLock : existedLocks) {
    		if (existedLock.getFirst() == tNum && existedLock.getSecond().lockType == lockType) {
    			throw new DuplicateLockRequestException("DuplicateLockRequestException");
    		}
    	}
    	
    	for (Pair<Long, Lock> existedLock : existedLocks) {
    		if (LockType.compatible(lockType, existedLock.getSecond().lockType) == false) {
    			comp = false;
    		}
    	}
    	
    	if (comp == true) {
        	Pair<Long, Lock> newPair = new Pair<>(tNum, newLock);
        	existedLocks.add(newPair);
        	resourceLocks.put(name, existedLocks);
        	//release
	    	for (ResourceName oldName:releaseLocks) {
	    		if (resourceLocks.containsKey(oldName) == false) {
	    			release = false;
	    			throw new NoLockHeldException("NoLockHeldException");
	    		}else {
	            	for (Pair<Long, Lock> existedLock : resourceLocks.get(oldName)) {
	            		if (existedLock.getFirst() == tNum) {
	            			find = true;
	            		}
	            	}
	            	if(find == false) {
	        			release = false;
	        			throw new NoLockHeldException("NoLockHeldException");
	            	}
		    		resourceLocks.remove(oldName);
		    		release = true;}
	    	}
    	}else {
    		transaction.block();
    		LockRequest lockReq = new LockRequest(transaction, newLock);
    		waitingQueue.addLast(lockReq);
    		if (releaseLocks.contains(name)){
    			resourceLocks.remove(name);
    			release = true;
    		}
    	}}
    	
    	while (release == true && !waitingQueue.isEmpty()) {
    		LockRequest lockcheck = waitingQueue.pop();
    		if (resourceLocks.get(name) == null) {
    			newcomp = true;
    		}else {
        	for (Pair<Long, Lock> existedLock : resourceLocks.get(name)) {
        		
        		if (LockType.compatible(lockcheck.lock.lockType, existedLock.getSecond().lockType) == false) {
        			newcomp = false;
        		}}
    		}
    		if (newcomp == true) {
    			lockcheck.transaction.unblock();
    			this.acquire(lockcheck.transaction, name, lockcheck.lock.lockType);
    		}else {
    			waitingQueue.addFirst(lockcheck);
    		}
        	
    	}

        //throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION. No error
     * checking is performed for holding requisite parent locks. Blocks the
     * transaction and places it in queue if the requested lock is not compatible
     * with another transaction's lock on the resource.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(BaseTransaction transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
    	
    	List<Pair<Long, Lock>> existedLocks =  resourceLocks.get(name);
    	Long tNum = transaction.getTransNum();
    	boolean comp  = true;
    	Lock newLock = new Lock(name, lockType);
    	
    	if (existedLocks == null) {
			List<Pair<Long, Lock>> newlist= new ArrayList<>();
	    	Pair<Long, Lock> newpair = new Pair<>(tNum, newLock);
	    	newlist.add(newpair);
	    	resourceLocks.put(name, newlist);
    	}else {
        	for (Pair<Long, Lock> existedLock : existedLocks) {
        		if (existedLock.getFirst() == tNum) {
        			throw new DuplicateLockRequestException("DuplicateLockRequestException");
        		}
        	}
        	
        	for (Pair<Long, Lock> existedLock : existedLocks) {
        		if (LockType.compatible(lockType, existedLock.getSecond().lockType) == false) {
        			comp = false;
        		}
        	}
        	
        	if (comp == true) {
            	Pair<Long, Lock> newPair = new Pair<>(tNum, newLock);
            	existedLocks.add(newPair);
            	resourceLocks.put(name, existedLocks);
        	}else {
        		transaction.block();
        		LockRequest lockReq = new LockRequest(transaction, newLock);
        		waitingQueue.addLast(lockReq);
        		//System.out.println(transaction);
        	}
    	}
    	
        //throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Release TRANSACTION's lock on NAME. No error checking is performed for
     * freeing dependent child locks. Unblocks and unqueues all transactions
     * that can be unblocked, in order of lock request.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(BaseTransaction transaction, ResourceName name)
    throws NoLockHeldException {
    	
    	boolean release  = false;
    	boolean find  = false;
    	boolean valid  = false;
    	boolean newcomp = true;
    	Long tNum = transaction.getTransNum();
    	
		if (resourceLocks.get(name) == null) {
			release = false;
			throw new NoLockHeldException("NoLockHeldException");
		}else{
        	for (Pair<Long, Lock> existedLock : resourceLocks.get(name)) {
        		if (existedLock.getFirst() == tNum) {
        			find = true;
        		}
        	}
        	if(find == false) {
    			release = false;
    			throw new NoLockHeldException("NoLockHeldException");
        	}
        	
        	List<Pair<Long, Lock>> newres= new ArrayList<>();
        	Iterator<Pair<Long, Lock>> it = resourceLocks.get(name).iterator();
        	while(it.hasNext()){
        		Pair<Long, Lock> existedLock = it.next();
        	    if(existedLock.getFirst() == tNum){
        	        it.remove();
        	    }else {
        	    newres.add(existedLock);}
        	}

        	resourceLocks.put(name, newres);
			//resourceLocks.remove(name);
			release = true;
			
			int count = 0;
			//System.out.println(waitingQueue);
    	while (release == true && !waitingQueue.isEmpty()) {
    		int i = waitingQueue.size();
    		//System.out.println(waitingQueue);
    		LockRequest lockcheck = waitingQueue.pop();
   
    		
    		if (resourceLocks.get(name) == null) {
    			newcomp = true;
    			//System.out.println("true");
    		}else {
        	for (Pair<Long, Lock> existedLock : resourceLocks.get(name)) {
        		if (LockType.compatible(lockcheck.lock.lockType, existedLock.getSecond().lockType) == false) {
        			newcomp = false;
        			//System.out.println("false");
        		}
        	}}
			Long newtn = null;
        	for (Pair<Long, Lock> existedLock : resourceLocks.get(name)) {
        		if (existedLock.getFirst() == lockcheck.transaction.getTransNum()) {
        			if (LockType.substitutable(lockcheck.lock.lockType, existedLock.getSecond().lockType)) {
        				valid = true;
        				newtn = existedLock.getFirst();
        			}
        		}
        	}
    		if (valid == true) {
    			//System.out.println(lockcheck);
    			lockcheck.transaction.unblock();
    			this.promote(lockcheck.transaction, name, lockcheck.lock.lockType);
    		}else {
    		if (newcomp == true) {
    			lockcheck.transaction.unblock();
//    			System.out.println(lockcheck.transaction);
//    			System.out.println(lockcheck.lock.lockType);
//    			System.out.println(this.getLocks(name));
    			this.acquire(lockcheck.transaction, name, lockcheck.lock.lockType);
    			//this.promote(transaction, name, lockcheck.lock.lockType);
    		}else {
    			//System.out.println(lockcheck);
    			//System.out.println(this.getLocks(name));
    			//waitingQueue.push(lockcheck);
    			waitingQueue.addFirst(lockcheck);
    			count += 1;
    			if (count > waitingQueue.size()) {
        			//System.out.println(lockcheck.transaction);
        			//System.out.println(lockcheck.lock.lockType);
        			//System.out.println(this.getLocks(name));
        			//System.out.println(this.getLocks(lockcheck.transaction));
    				break;
    			}
    		}
        	}}
    	}

        //throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE. No error checking is
     * performed for holding requisite locks. Blocks the transaction and places
     * TRANSACTION in the **front** of the queue if the request cannot be
     * immediately granted (i.e. another transaction holds a conflicting lock). A
     * lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(BaseTransaction transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
    	boolean find  = false;
    	boolean comp = true;
    	boolean valid = false;
    	Long tNum = transaction.getTransNum();
    	
		if (resourceLocks.get(name) == null) {
			throw new NoLockHeldException("NoLockHeldException");
		}else{
        	for (Pair<Long, Lock> existedLock : resourceLocks.get(name)) {
        		if (LockType.compatible(newLockType, existedLock.getSecond().lockType) == false && existedLock.getFirst() != tNum) {
        			comp = false;
        		}
        	}
        	//System.out.println(resourceLocks.get(name));
        	for (Pair<Long, Lock> existedLock : resourceLocks.get(name)) {
        		if (existedLock.getFirst() == tNum) {
        			find = true;
        			if (existedLock.getSecond().lockType == newLockType) {
        				throw new DuplicateLockRequestException("DuplicateLockRequestException");
        			}
        			if (LockType.substitutable(newLockType, existedLock.getSecond().lockType)) {
        				valid = true;
        			}else {
        				throw new InvalidLockException("InvalidLockException");
        			}
        		}
        	}
        	Lock newlock = new Lock(name, newLockType);
			if (valid == true) {
			if (comp == true) {
				List<Pair<Long, Lock>> existedLocks =  resourceLocks.get(name);
				for (Pair<Long, Lock> existedLock : existedLocks){
					if (existedLock.getFirst() == tNum) {
						existedLocks.remove(existedLock);
						break;
					}
				}
				//Lock newlock = new Lock(name, newLockType);
				Pair<Long, Lock> newPair = new Pair<>(tNum, newlock);
				
            	existedLocks.add(newPair);
            	resourceLocks.put(name, existedLocks);
			}else {
        		transaction.block();
        		LockRequest lockReq = new LockRequest(transaction, newlock);
        		waitingQueue.addFirst(lockReq);
			}}
        	if(find == false) {
    			throw new NoLockHeldException("NoLockHeldException");
        	}
		}
        //throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Return the type of lock TRANSACTION has on NAME, or null if no lock is
     * held.
     */
    public LockType getLockType(BaseTransaction transaction, ResourceName name) {
    	LockType lockType = null;
    	List<Pair<Long, Lock>> existedLocks =  resourceLocks.get(name);
    	Long tNum = transaction.getTransNum();
    	if (existedLocks == null) {
    		return null;
    	}else {
	    	for (Pair<Long, Lock> existedLock : existedLocks) {
	    		if (existedLock.getFirst() == tNum) {
	    			lockType = existedLock.getSecond().lockType;
	    		}
	    	}
    	}
    	return lockType;
        //throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Returns the list of transactions ids and lock types for locks held on
     * NAME, in order of acquisition. A promotion should count as acquired
     * at the original time.
     */
    public List<Pair<Long, LockType>> getLocks(ResourceName name) {
    	List<Pair<Long, LockType>> result = new ArrayList<>();
    	List<Pair<Long, Lock>> existedLocks =  resourceLocks.get(name);
    	
    	if (existedLocks != null) {
	    	for (Pair<Long, Lock> existedLock : existedLocks) {
	    		Long tid = existedLock.getFirst();
	    		LockType locktype = existedLock.getSecond().lockType;
	    		Pair<Long, LockType> newpair = new Pair<>(tid, locktype);
	    		result.add(newpair);	
	    	}
    	}
    	return result;
        //throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Returns the list of resource names and lock types for locks held by
     * TRANSACTION, in order of acquisition. A promotion should count as acquired
     * at the original time.
     */
    public List<Pair<ResourceName, LockType>> getLocks(BaseTransaction transaction) {
    	List<Pair<ResourceName, LockType>> result = new ArrayList<>();
    	Long tNum = transaction.getTransNum();
    	
    	for (ResourceName existedname: resourceLocks.keySet()) {
    		List<Pair<Long, Lock>> existedLocks = resourceLocks.get(existedname);
        	if (existedLocks != null) {
        		//System.out.println(existedLocks);
        		//System.out.println(tNum);
    	    	for (Pair<Long, Lock> existedLock : existedLocks) {
    	    		if (existedLock.getFirst() == tNum) {
    	    			//System.out.println("entered");
    	    			//System.out.println(tNum);
    	    			LockType locktype = existedLock.getSecond().lockType;
    	    			//System.out.println(locktype);
    	    			Pair<ResourceName, LockType> newpair = new Pair<>(existedname, locktype);
    	    			result.add(newpair);
    	    		}
    	    	}
        	}
    	}
    	return result;
        //throw new UnsupportedOperationException("TODO(hw5): implement");
    }
}
