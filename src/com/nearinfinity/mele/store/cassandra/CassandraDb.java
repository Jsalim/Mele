/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/**
 * @author Aaron McCurry (amccurry@nearinfinity.com)
 */
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
