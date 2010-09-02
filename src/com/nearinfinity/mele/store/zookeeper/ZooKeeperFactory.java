package com.nearinfinity.mele.store.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.mele.store.MeleConfiguration;

public class ZooKeeperFactory {

	private static ZooKeeper zk;

	public static ZooKeeper create(MeleConfiguration configuration, Watcher watcher) throws IOException {
		if (configuration.isZooKeeperEmbedded()) {
			if (isServerNode(configuration)) {
				startZooKeeperServer();
			}
		}
		return zk = new ZooKeeper(configuration.getZooKeeperConnectionString(),
				configuration.getZooKeeperSessionTimeout(), watcher);
	}

	private static boolean isServerNode(MeleConfiguration configuration) {
		return false;
	}

	private static void startZooKeeperServer() {
		
	}
	
	public static synchronized ZooKeeper getZooKeeper() {
		return zk;
	}
}
