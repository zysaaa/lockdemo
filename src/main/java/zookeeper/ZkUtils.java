package zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.Objects;

/**
 * Created by caoweibo on 2020/1/25.
 */
public class ZkUtils {

    public static void checkPath(String path, ZooKeeper client) throws KeeperException, InterruptedException {
        Objects.requireNonNull(path);
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("Path should starts with / !");
        }
        // 是否已经存在
        if (client.exists(path, false) != null) {
            throw new IllegalArgumentException("Path already exists !");
        }
    }

    public static void createPersistNodes(String path, ZooKeeper client) throws KeeperException, InterruptedException {
        if (client.exists(path, false) != null) {
            return;
        }
        String pPath = path.substring(0, path.lastIndexOf("/"));
        if (pPath.length() == 0) {
            client.create(path, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            createPersistNodes(pPath, client);
            client.create(path, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
}
