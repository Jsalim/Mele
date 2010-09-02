package com.nearinfinity.mele;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;

import com.nearinfinity.mele.store.MeleConfiguration;
import com.nearinfinity.mele.store.MeleController;

public abstract class Mele {
	
	public static Mele getMele() {
		return getMele(new MeleConfiguration());
	}
	
	public static Mele getMele(MeleConfiguration configuration) {
		return MeleController.getInstance(configuration);
	} 
	
	public abstract IndexDeletionPolicy getIndexDeletionPolicy(String directoryCluster, String directoryName) throws IOException;
	public abstract Directory open(String directoryCluster, String directoryName) throws IOException;
	public abstract void createDirectoryCluster(String directoryCluster) throws IOException;
	public abstract void createDirectory(String directoryCluster, String directoryName) throws IOException;
	public abstract List<String> listClusters() throws IOException;
	public abstract List<String> listDirectories(String directoryCluster) throws IOException;
	public abstract List<String> listLocalDirectories(String directoryCluster) throws IOException;
	public abstract void removeDirectoryCluster(String directoryCluster) throws IOException;
	public abstract void removeDirectory(String directoryCluster, String directoryName) throws IOException;
	
}
