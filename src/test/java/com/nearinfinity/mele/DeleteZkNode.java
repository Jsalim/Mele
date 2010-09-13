package com.nearinfinity.mele;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.mele.store.zookeeper.NoOpWatcher;
import com.nearinfinity.mele.store.zookeeper.ZooKeeperFactory;

public class DeleteZkNode {

    public static void main(String[] args) throws IOException {
        ZooKeeperFactory.create(new MeleConfiguration(), new NoOpWatcher());
        ZooKeeper zk = ZooKeeperFactory.getZooKeeper();
        deleteAnyVersion(zk, "/mele");
    }

    public static void deleteAnyVersion(ZooKeeper zk, String path) {
        try {
            List<String> children = zk.getChildren(path, false);
            for (String c : children) {
                deleteAnyVersion(zk, path + "/" + c);
            }
            zk.delete(path, -1);
        }
        catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                return;
            }
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
