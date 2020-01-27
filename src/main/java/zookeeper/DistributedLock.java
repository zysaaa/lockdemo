package zookeeper;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Created by caoweibo on 2020/1/25.
 */
public interface DistributedLock extends Lock {

    @Override
    default void lockInterruptibly() throws InterruptedException {
        throw new UnsupportDistributedLockException("Lock interruptibly unsupported!");
    }

    @Override
    default Condition newCondition() {
        throw new UnsupportDistributedLockException("Condition unsupported!");
    }

    class UnsupportDistributedLockException extends RuntimeException {
        public UnsupportDistributedLockException(String msg, Exception e) {
            super(msg, e);
        }

        public UnsupportDistributedLockException(String msg) {
            super(msg);
        }
    }

    class DistributedLockingException extends RuntimeException {
        public DistributedLockingException(String msg, Exception e) {
            super(msg, e);
        }

        public DistributedLockingException(String msg) {
            super(msg);
        }
    }
}
