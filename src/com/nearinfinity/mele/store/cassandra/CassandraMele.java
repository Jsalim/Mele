package com.nearinfinity.mele.store.cassandra;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.nearinfinity.mele.store.BaseMele;
import com.nearinfinity.mele.store.MeleConfiguration;

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