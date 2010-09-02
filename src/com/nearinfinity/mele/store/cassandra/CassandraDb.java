package com.nearinfinity.mele.store.cassandra;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.lucene.store.Directory;

import com.nearinfinity.mele.store.BaseMele;
import com.nearinfinity.mele.store.MeleConfiguration;
import com.nearinfinity.mele.store.MeleDirectory;
import com.nearinfinity.mele.store.MeleDirectoryStore;
import com.nearinfinity.mele.store.cassandra.storageproxy.CassandraStorageProxyStore;
import com.nearinfinity.mele.store.cassandra.thrift.CassandraExecutor;
import com.nearinfinity.mele.store.cassandra.thrift.CassandraThriftDirectoryStore;
import com.nearinfinity.mele.store.zookeeper.ZookeeperWrapperDirectory;

public abstract class CassandraDb {

	public abstract void delete(String directoryCluster, String directory);

	public abstract Directory open(String directoryCluster, String directoryName);
	
	public static synchronized CassandraDb create(MeleConfiguration configuration) {
		if (configuration.isCassandraEmbedded()) {
			return new CassandraDbEmbedded(configuration);
		} else {
			return new CassandraDbRemote(configuration);
		}
	}
	
	static abstract class CassandraDbBase extends CassandraDb {
		
		protected MeleConfiguration configuration;
		protected String keyspace;
		protected String columnFamily;
		protected ConsistencyLevel consistencyLevel;
		
		public CassandraDbBase(MeleConfiguration configuration) {
			this.configuration = configuration;
			consistencyLevel = ConsistencyLevel.valueOf(configuration.getCassandraConsistencyLevel());
			keyspace = configuration.getCassandraKeyspace();
			columnFamily = configuration.getCassandraColumnFamily();
		}
		
		@Override
		public Directory open(String directoryCluster, String directoryName) {
			MeleDirectoryStore store = openInternal(directoryCluster,directoryName);
			MeleDirectory directory = new MeleDirectory(store);
			return new ZookeeperWrapperDirectory(directory,
					BaseMele.getReferencePath(configuration,directoryCluster,directoryName),
					BaseMele.getLockPath(configuration,directoryCluster,directoryName));
		}
		
		public abstract MeleDirectoryStore openInternal(String directoryCluster, String directoryName);
		


		protected String getDirectoryName(String directoryCluster, String directoryName) {
			return directoryCluster + "/" + directoryName;
		}
		
	}
	
	static class CassandraDbRemote extends CassandraDbBase {

		private int poolSize;
		private String hostname;
		private int port;

		public CassandraDbRemote(MeleConfiguration configuration) {
			super(configuration);
			poolSize = configuration.getCassandraRemotePoolSize();
			hostname = configuration.getCassandraRemoteHostname();
			port = configuration.getCassandraRemotePort();
			CassandraExecutor.setup(port, poolSize, hostname);
		}

		@Override
		public void delete(String directoryCluster, String directory) {
			
		}

		@Override
		public MeleDirectoryStore openInternal(String directoryCluster, String directoryName) {
			return new CassandraThriftDirectoryStore(keyspace,columnFamily,
					getDirectoryName(directoryCluster,directoryName),consistencyLevel);
		}

	}
	
	static class CassandraDbEmbedded extends CassandraDbBase {

		public CassandraDbEmbedded(MeleConfiguration configuration) {
			super(configuration);
			CassandraStorageProxyStore.start();
		}

		@Override
		public void delete(String directoryCluster, String directory) {
			
		}

		@Override
		public MeleDirectoryStore openInternal(String directoryCluster, String directoryName) {
			return new CassandraStorageProxyStore(keyspace,columnFamily,
					getDirectoryName(directoryCluster,directoryName),consistencyLevel);
		}
	}
}
