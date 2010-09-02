package com.nearinfinity.mele.store.rsync;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.mele.store.MeleConfiguration;
import com.nearinfinity.mele.store.util.ZkUtils;
import com.nearinfinity.mele.store.zookeeper.ZooKeeperFactory;

public class RsyncDecision {
	
	private ZooKeeper zk;
	private MeleConfiguration configuration;
	private int replicationFactor = 0;
	private String livePath;
	private String registeredPath;
	private String localAddress;
	private String localIpAddress;
	private String directoriesPath;

	public RsyncDecision(MeleConfiguration configuration) {
		this.configuration = configuration;
		this.zk = ZooKeeperFactory.getZooKeeper();
		this.localAddress = configuration.getRsyncLocalAddress();
		this.livePath = configuration.getRsyncZooKeeperAddressLivePath();
		this.registeredPath = configuration.getRsyncZooKeeperAddressRegisteredPath();
		this.directoriesPath = configuration.getRsyncZooKeeperDirectoriesPath();
		this.replicationFactor = configuration.getRsyncReplicationFactor();
		ZkUtils.mkNodesStr(zk, livePath);
		ZkUtils.mkNodesStr(zk, registeredPath);
		registerThisNode();
	}
	
	private void registerThisNode() {
		try {
			localIpAddress = getIpAddress(localAddress);
			zk.create(ZkUtils.getPath(livePath,localIpAddress), null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			Stat stat = zk.exists(ZkUtils.getPath(registeredPath,localIpAddress), false);
			if (stat == null) {
				zk.create(ZkUtils.getPath(registeredPath,localIpAddress), configuration.getRsyncBaseDir().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public String getRemoteDirectory(String host) {
		String ipAddress = getIpAddress(host);
		try {
			return new String(zk.getData(ZkUtils.getPath(registeredPath,ipAddress), false, null));
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public List<String> getRemoteAddresses(String directoryCluster, String directoryName) {
		List<String> registeredAddresses = getRegisteredAddresses();
		List<String> liveAddresses = getLiveAddresses();
		return getRemoteAddresses(directoryCluster,directoryName,registeredAddresses,liveAddresses);
	}
	
	public List<String> getRemoteAddresses(String directoryCluster, String directoryName,
			List<String> registeredAddresses, List<String> liveAddresses) {
		if (replicationFactor <= 1) {
			return new ArrayList<String>();
		}
		//find if directory is currently being served by other servers
		List<String> otherNodes = findOtherNodesServing(directoryCluster,directoryName);
		
		//@todo if any are dead decide whether or not to migrate data
		while (otherNodes.size() < replicationFactor - 1) {
			otherNodes.add(randomChooseAnotherNode(otherNodes));
		}
		return otherNodes;
	}
	
	private Random random = new Random();

	private String randomChooseAnotherNode(List<String> nodes) {
		List<String> liveNodes = new ArrayList<String>(getLiveAddresses());
		liveNodes.remove(localIpAddress);
		liveNodes.removeAll(nodes);
		if (liveNodes.size() == 0) {
			throw new RuntimeException("i give up!, too many dead");
		} else if (liveNodes.size() == 1) {
			return liveNodes.get(0);
		}
		
		return liveNodes.get(random.nextInt(liveNodes.size()));
	}

	private List<String> findOtherNodesServing(String directoryCluster, String directoryName) {
		List<String> result = new ArrayList<String>();
		try {
			List<String> children = zk.getChildren(ZkUtils.getPath(directoriesPath,directoryCluster,directoryName), false);
			result.addAll(children);
			result.remove(localIpAddress);
			return result;
		} catch (KeeperException e) {
			if (e.code() == Code.NONODE) {
				return result;
			}
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public List<String> getLiveAddresses() {
		try {
			return zk.getChildren(livePath, false);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public List<String> getRegisteredAddresses() {
		try {
			return zk.getChildren(registeredPath, false);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	private String getIpAddress(String host) {
		try {
			InetAddress address = InetAddress.getByName(host);
			return address.getHostAddress();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	public void setDirectoryLocation(String directoryCluster, String directoryName, String address) {
		ZkUtils.mkNodes(zk, directoriesPath,directoryCluster,directoryName,address);
	}
}
