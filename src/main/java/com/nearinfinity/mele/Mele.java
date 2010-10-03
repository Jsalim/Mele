package com.nearinfinity.mele;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;

public interface Mele {

    IndexDeletionPolicy getIndexDeletionPolicy(String directoryCluster, String directoryName) throws IOException;

    Directory open(String directoryCluster, String directoryName) throws IOException;

    void createDirectoryCluster(String directoryCluster);

    void createDirectory(String directoryCluster, String directoryName);

    List<String> listClusters();

    List<String> listDirectories(String directoryCluster);

    List<String> listLocalDirectories(String directoryCluster);

    void removeDirectoryCluster(String directoryCluster) throws IOException;

    void removeDirectory(String directoryCluster, String directoryName) throws IOException;

}