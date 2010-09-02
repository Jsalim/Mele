package com.nearinfinity.mele.store.zookeeper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.mele.store.util.ZkUtils;

public class ZookeeperLockFactory extends LockFactory {
	
	private final static Log LOG = LogFactory.getLog(ZookeeperLockFactory.class);
	private ZooKeeper zk;
	private String indexLockPath;
	
	public ZookeeperLockFactory(ZooKeeper zk, String indexLockPath) {
		this.indexLockPath = indexLockPath;
		this.zk = zk;
		ZkUtils.mkNodesStr(zk,indexLockPath);
	}

	@Override
	public void clearLock(String lockName) throws IOException {
		LOG.info("clear lock.... [" + lockName + "]");
	}

	@Override
	public Lock makeLock(String lockName) {
		try {
			return new ZookeeperLock(zk, indexLockPath, lockName);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class ZookeeperLock extends Lock {
		
		private ZooKeeper zk;
		private String instanceIndexLockPath;
		private Stat exists;

		public ZookeeperLock(ZooKeeper zk, String indexLockPath, String name) throws IOException {
			ZkUtils.mkNodesStr(zk, indexLockPath);
			this.zk = zk;
			this.instanceIndexLockPath = ZkUtils.getPath(indexLockPath,name);
		}

		@Override
		public boolean isLocked() throws IOException {
			try {
				if (zk.getChildren(instanceIndexLockPath, false).size() > 0) {
					return true;
				}
			} catch (KeeperException e) {
				throw new IOException(e);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			return false;
		}

		@Override
		public boolean obtain() throws IOException {
			try {
				zk.create(instanceIndexLockPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				exists = zk.exists(instanceIndexLockPath, false);
				return true;
			} catch (KeeperException e) {
				if (e.code() == Code.NODEEXISTS) {
					return false;
				}
				throw new IOException(e);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void release() throws IOException {
			try {
				zk.delete(instanceIndexLockPath, exists.getVersion());
			} catch (InterruptedException e) {
				throw new IOException(e);
			} catch (KeeperException e) {
				throw new IOException(e);
			}
		}

	}
}
