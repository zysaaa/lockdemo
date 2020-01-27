package zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

/**
 * Created by caoweibo on 2020/1/27.
 */
public class ZkTest {

    private static final String LOCK_PATH = "/lockdemo/locktest";

    private static final String ZK_ADDR = "127.0.0.1:2181";

    private static ZooKeeper client;

    private static final ExecutorService pool = Executors.newCachedThreadPool();

    @Before
    public void setup() throws InterruptedException, IOException {
        CountDownLatch latch = new CountDownLatch(1);
        client = new ZooKeeper(ZK_ADDR, 60000, watchedEvent -> {
            if (watchedEvent.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                latch.countDown();
            }
        });
        latch.await();
    }

    @After
    public void clear() throws InterruptedException {
        client.close();
    }

    // test util create nodes
    @Test
    public void testCreateNodes() throws KeeperException, InterruptedException {
        ZkUtils.createPersistNodes("/test1/test11/test111", client);
        assertTrue("Path should exists!",
                client.exists("/test1/test11/test111", false) != null);
    }

    @Test
    public void test() throws ExecutionException, InterruptedException, TimeoutException {
        Lock lock = new DistributedLockImpl(generatePath(), ZK_ADDR);
        lock.lock();
        lock.lock();
        lock.unlock();
        lock.unlock();

        Callable<Object> callable = () -> {
            lock.lock();
            Thread.sleep(3000);
            lock.unlock();
            return new Object();
        };
        pool.submit(callable);
        // 确保先执行 callable
        Thread.sleep(100);
        assertFalse("Should not get lock",
                lock.tryLock(2, TimeUnit.SECONDS));
        assertTrue("Should get lock",
                lock.tryLock(1, TimeUnit.SECONDS));
        lock.unlock();
    }

    @Test
    public void test2() {
        Lock lock = new DistributedLockImpl(generatePath(), ZK_ADDR);
        try {
            lock.unlock();
            fail("Exception expected.");
        } catch (DistributedLock.DistributedLockingException e) {
            // expected
        }
    }

    @Test
    public void test3() throws InterruptedException {
        long start = System.currentTimeMillis();
        Lock lock = new DistributedLockImpl(generatePath(), ZK_ADDR);
        new Thread(() -> {
            lock.lock();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
            lock.unlock();
        }).start();
        Thread.sleep(100);
        lock.lock();
        if (System.currentTimeMillis() - start < 2 * 1000) {
            fail("Should not get lock in main thread.");
        }
        lock.unlock();
    }

    private static String generatePath() {
        return LOCK_PATH + UUID.randomUUID().toString();
    }
}
