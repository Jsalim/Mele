package com.nearinfinity.mele;

public interface MeleConstants {

    public static final String MELE_ZOOKEEPER_LOCK_NAME = "locks";
    public static final String MELE_ZOOKEEPER_REFS_NAME = "refs";

    // setup in mele-site.properties
    public static final String MELE_ZOOKEEPER_CONNECTION = "mele.zookeeper.connection";
    public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 3000;
    public static final String MELE_ZOOKEEPER_SESSION_TIMEOUT = "mele.zookeeper.session.timeout";
    public static final String MELE_BASE_ZOOKEEPER_PATH = "mele.base.zookeeper.path";
    public static final String MELE_LOCAL_REPLICATION_PATHS = "mele.local.replication.paths";
    public static final String MELE_BASE_HDFS_PATH = "mele.base.hdfs.path";
    
    public static final String MELE_CASSANDRA_COLUMNFAMILY = "mele.cassandra.columnfamily";
    public static final String MELE_CASSANDRA_POOLSIZE = "mele.cassandra.poolsize";
    public static final String MELE_CASSANDRA_HOSTNAME = "mele.cassandra.hostname";
    public static final String MELE_CASSANDRA_PORT = "mele.cassandra.port";
    public static final String MELE_CASSANDRA_KEYSPACE = "mele.cassandra.keyspace";
}
