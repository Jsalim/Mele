package com.nearinfinity.mele.store;

import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.hadoop.fs.FileSystem;

public class MeleConfiguration {

	private String rsyncBaseDir;
	private String rsyncLocalAddress;
	private int rsyncReplicationFactor;
	private List<String> localReplicationPathList;
	private String baseHdfsPath;
	private FileSystem hdfsFileSystem;
	private boolean usingHdfs;

	public String getZooKeeperConnectionString() {
		return "localhost";
	}

	public int getZooKeeperSessionTimeout() {
		return 3000;
	}

	public boolean isCassandraEmbedded() {
		return false;
	}

	public boolean isZooKeeperEmbedded() {
		return false;
	}

	public String getBaseZooKeeperPath() {
		return "/mele";
	}

	public String getZooKeeperReferenceNodeName() {
		return "refs";
	}

	public String getZooKeeperLockNodeName() {
		return "locks";
	}

	public int getCassandraRemotePoolSize() {
		return 10;
	}

	public String getCassandraRemoteHostname() {
		return "localhost";
	}

	public int getCassandraRemotePort() {
		return 9160;
	}

	public String getCassandraConsistencyLevel() {
		return ConsistencyLevel.ONE.name();
	}

	public String getCassandraKeyspace() {
		return "Keyspace1";
	}

	public String getCassandraColumnFamily() {
		return "Standard1";
	}

	public boolean isUsingCassandra() {
		return false;
	}

	public boolean isUsingRync() {
		return false;
	}

	public String getRsyncBaseDir() {
		return rsyncBaseDir;
	}

	public void setRsyncBaseDir(String rsyncBaseDir) {
		this.rsyncBaseDir = rsyncBaseDir;
	}

	public String getRsyncZooKeeperAddressRegisteredPath() {
		return "/mele/rsync/nodes/registered";
	}
	
	public String getRsyncZooKeeperAddressLivePath() {
		return "/mele/rsync/nodes/live";
	}
	
	public String getRsyncZooKeeperDirectoriesPath() {
		return "/mele/rsync/dirs";
	}

	public String getRsyncLocalAddress() {
		return rsyncLocalAddress;
	}

	public void setRsyncLocalAddress(String rsyncLocalAddress) {
		this.rsyncLocalAddress = rsyncLocalAddress;
	}

	public int getRsyncReplicationFactor() {
		return rsyncReplicationFactor;
	}

	public void setRsyncReplicationFactor(int rsyncReplicationFactor) {
		this.rsyncReplicationFactor = rsyncReplicationFactor;
	}

	public List<String> getLocalReplicationPathList() {
		return localReplicationPathList;
	}

	public String getBaseHdfsPath() {
		return baseHdfsPath;
	}

	public FileSystem getHdfsFileSystem() {
		return hdfsFileSystem;
	}

	public boolean isUsingHdfs() {
		return usingHdfs;
	}

	public void setLocalReplicationPathList(List<String> localReplicationPathList) {
		this.localReplicationPathList = localReplicationPathList;
	}

	public void setBaseHdfsPath(String baseHdfsPath) {
		this.baseHdfsPath = baseHdfsPath;
	}

	public void setHdfsFileSystem(FileSystem hdfsFileSystem) {
		this.hdfsFileSystem = hdfsFileSystem;
	}

	public void setUsingHdfs(boolean usingHdfs) {
		this.usingHdfs = usingHdfs;
	}
}
