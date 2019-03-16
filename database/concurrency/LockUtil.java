package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;

import java.util.concurrent.locks.Lock;

public class LockUtil {
    /**
     * Ensure that TRANSACTION can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType must be one of LockType.S, LockType.X, and behavior is unspecified
     * if an intent lock is passed in to this method (you can do whatever you want in this case).
     *
     * If TRANSACTION is null, this method should do nothing.
     */
    public static void requestLocks(BaseTransaction transaction, LockContext lockContext,
                                    LockType lockType) {
    	if (transaction == null) {
    		return;
    	}
    	
        //throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    // TODO(hw5): add helper methods as you see fit
}
