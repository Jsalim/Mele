package com.nearinfinity.mele.store.db.cassandra;

import java.io.IOException;

import org.apache.lucene.store.Directory;

import com.nearinfinity.mele.MeleConfiguration;
import com.nearinfinity.mele.MeleDirectoryFactory;
import com.nearinfinity.mele.store.db.MeleDirectory;
import com.nearinfinity.mele.store.db.MeleDirectory.BLOCK_SIZE;

public class CassandraDirectoryFactory implements MeleDirectoryFactory {
    
    private String cassandraKeySpace;
    private String cassandraColumnFamily;
    private int cassandraPoolSize;
    private String cassandraHostName;
    private int cassandraPort;

    public CassandraDirectoryFactory(MeleConfiguration configuration) throws IOException {
        cassandraKeySpace = configuration.getCassandraKeySpace();
        cassandraColumnFamily = configuration.getCassandraColumnFamily();
        cassandraPoolSize = configuration.getCassandraPoolSize();
        cassandraHostName = configuration.getCassandraHostName();
        cassandraPort = configuration.getCassandraPort();
    }

    public Directory getDirectory(String directoryCluster, String directoryName) throws IOException {
        CassandraStore cassandraStore = new CassandraStore(cassandraKeySpace, 
                cassandraColumnFamily, directoryCluster + "/" + directoryName, cassandraPoolSize, cassandraHostName, cassandraPort);
        return new MeleDirectory(cassandraStore, BLOCK_SIZE._16K);
    }
}
