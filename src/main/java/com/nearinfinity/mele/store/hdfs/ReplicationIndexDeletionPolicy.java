package com.nearinfinity.mele.store.hdfs;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;
import static com.nearinfinity.mele.store.hdfs.HdfsDirectory.copyFile;

public class ReplicationIndexDeletionPolicy implements IndexDeletionPolicy {
	
	private IndexDeletionPolicy primaryIndexDeletionPolicy;
	private Directory remoteDirectory;
	private Directory localDirectory;
	
	public ReplicationIndexDeletionPolicy(IndexDeletionPolicy policy, Directory localDirectory, Directory remoteDirectory) {
		this.primaryIndexDeletionPolicy = policy;
		this.localDirectory = localDirectory;
		this.remoteDirectory = remoteDirectory;
	}
	
	private void sync(List<? extends IndexCommit> commits) throws IOException {
		for (IndexCommit commit : commits) {
			Collection<String> fileNames = commit.getFileNames();
			for (String name : fileNames) {
				copyFile(name, localDirectory, remoteDirectory);
			}
		}
	}

	@Override
	public void onInit(List<? extends IndexCommit> commits) throws IOException {
		primaryIndexDeletionPolicy.onInit(commits);
		sync(commits);
	}

	@Override
	public void onCommit(List<? extends IndexCommit> commits) throws IOException {
		primaryIndexDeletionPolicy.onCommit(commits);
		sync(commits);
	}
}
