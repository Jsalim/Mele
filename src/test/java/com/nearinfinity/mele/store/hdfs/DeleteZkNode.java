package com.nearinfinity.mele.store.hdfs;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;

import com.nearinfinity.mele.store.MeleConfiguration;
import com.nearinfinity.mele.store.zookeeper.ZooKeeperFactory;

public class DeleteZkNode {

	public static void main(String[] args) throws IOException {
		ZooKeeperFactory.create(new MeleConfiguration(), new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
		ZooKeeper zk = ZooKeeperFactory.getZooKeeper();
		delete(zk,"/mele");
	}

	public static void delete(ZooKeeper zk, String path) {
		try {
			List<String> children = zk.getChildren(path, false);
			for (String c : children) {
				delete(zk,path + "/" + c);
			}
			zk.delete(path, -1);
		} catch (KeeperException e) {
			if (e.code() == Code.NONODE) {
				return;
			}
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

}
