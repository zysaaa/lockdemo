package zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by caoweibo on 2020/1/25.
 */
public class DistributedLockImpl implements DistributedLock {

    private static ZooKeeper zk;

    private String zkAddr;

    private int sessionTimeout = 60000; // default 60s

    private String lockPath;

    private ConcurrentMap<Thread, AtomicInteger> threadData = new ConcurrentHashMap<>();

    private static ThreadLocal<String> lockedPath = new ThreadLocal<>();

    public DistributedLockImpl(String lockPath, String zkAddr) {
        this.lockPath = lockPath;
        this.zkAddr = zkAddr;
        init();
    }

    private void init() {
        try {
            if (!initClient()) {
                throw new DistributedLockingException("Init zookeeper client error.");
            }
            ZkUtils.checkPath(lockPath, zk);
            createPathNode();
        } catch (Exception e) {
            throw new DistributedLockingException("Init zookeeper client error.");
        }
    }

    private boolean initClient() throws InterruptedException, IOException {
        CountDownLatch latch = new CountDownLatch(1);
        zk = new ZooKeeper(zkAddr, sessionTimeout, watchedEvent -> {
            if (watchedEvent.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                latch.countDown();
            }
        });
        return latch.await(5, TimeUnit.SECONDS);
    }

    private void createPathNode() throws KeeperException, InterruptedException {
        ZkUtils.createPersistNodes(lockPath, zk);
    }

    private String createCurrentNode() throws KeeperException, InterruptedException {
        // 创建临时子节点
        return zk.create(lockPath + "/forLock_", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    @Override
    public void lock() {
        // 重入
        if (threadData.get(Thread.currentThread()) != null) {
            threadData.get(Thread.currentThread()).incrementAndGet();
            return;
        }
        try {
            String currentNode = createCurrentNode();
            tryGetLock(currentNode, -1, null);
        } catch (Exception e) {
            throw new DistributedLockingException("Error when locking.", e);
        }
    }

    private boolean tryGetLock
                            (String myNodePath,
                            long time,
                            TimeUnit unit) throws Exception {
        final long millisToWait = (unit != null) ? unit.toMillis(time) : -1;
        long deadline = millisToWait + System.currentTimeMillis();
        while (lockedPath.get() == null &&
                (millisToWait == -1 || deadline > System.currentTimeMillis())) {
            // 获取根节点下的所有，查看序号
            List<String> subNodes = zk.getChildren(lockPath, false);
            subNodes.sort(String::compareTo);
            // 是否获取到锁
            if (myNodePath.endsWith(subNodes.get(0))) {
                // 获取到锁
                lockedPath.set(myNodePath);
                threadData.put(Thread.currentThread(), new AtomicInteger(1));
                return true;
            } else {
                // watch and wait
                for (int i = 1; i < subNodes.size(); i++) {
                    if (myNodePath.endsWith(subNodes.get(i))) {
                        CountDownLatch tmpLatch = new CountDownLatch(1);
                        Stat exists = zk.exists(lockPath + "/" + subNodes.get(i - 1), event -> {
                            if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
                                tmpLatch.countDown();
                            }
                        });
                        if (exists != null) {
                            if (millisToWait == -1) {
                                tmpLatch.await();
                            } else {
                                tmpLatch.await(time, unit);
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    public boolean tryLock() {
        try {
            // 重入
            if (threadData.get(Thread.currentThread()) != null) {
                threadData.get(Thread.currentThread()).incrementAndGet();
                return true;
            }
            String currentNode = createCurrentNode();
            // 获取根节点下的所有，查看序号
            List<String> subNodes = zk.getChildren(lockPath, false);
            subNodes.sort(String::compareTo);
            // 是否获取到锁
            if (currentNode.endsWith(subNodes.get(0))) {
                // 获取到锁
                lockedPath.set(currentNode);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new DistributedLockingException("Error when lock.", e);
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        try {
            String currentNode = createCurrentNode();
            boolean getLock = tryGetLock(currentNode, time, unit);
            if (!getLock) {
                zk.delete(currentNode, -1);
            }
            return getLock;
        } catch (Exception e) {
            throw new DistributedLockingException("Error when locking.", e);
        }
    }

    @Override
    public void unlock() {
        if (lockedPath.get() == null) {
            throw new DistributedLockingException("Can not unlock since not locked.");
        }
        if (threadData.get(Thread.currentThread()).intValue() > 1) {
            threadData.get(Thread.currentThread()).decrementAndGet();
        } else {
            try {
                Stat exists = zk.exists(lockedPath.get(), false);
                if (exists == null) {
                    // warn log and ignore
                } else {
                    zk.delete(lockedPath.get(), -1);
                    lockedPath.remove();
                    threadData.remove(Thread.currentThread());
                }
            } catch (Exception e) {
                throw new DistributedLockingException("Error when unlock.", e);
            }
        }

    }
}
