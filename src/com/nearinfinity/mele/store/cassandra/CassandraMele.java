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

import java.io.IOException;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.nearinfinity.mele.store.BaseMele;
import com.nearinfinity.mele.store.MeleConfiguration;

/**
 * @author Aaron McCurry (amccurry@nearinfinity.com)
 */
public class CassandraMele extends BaseMele implements Watcher {
	
	private CassandraDb cassandraDb;

	public CassandraMele(MeleConfiguration configuration) throws IOException {
		super(configuration);
		this.cassandraDb = CassandraDb.create(configuration);
	}

	@Override
	protected void internalDelete(String directoryCluster, String directoryName) {
		cassandraDb.delete(directoryCluster, directoryName);
	}

	@Override
	protected Directory internalOpen(String directoryCluster, String directoryName) {
		return cassandraDb.open(directoryCluster, directoryName);
	}

	@Override
	public void process(WatchedEvent event) {
		
	}

	@Override
	public List<String> listLocalDirectories(String directoryCluster) {
		return listDirectories(directoryCluster);
	}
}