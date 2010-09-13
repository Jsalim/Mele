package com.nearinfinity.mele.store.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

    public ReplicationIndexDeletionPolicy(IndexDeletionPolicy policy, Directory localDirectory,
                                          Directory remoteDirectory) throws IOException {
        this.primaryIndexDeletionPolicy = policy;
        this.localDirectory = localDirectory;
        this.remoteDirectory = remoteDirectory;
        updateLocalDirectory();
    }

    private void updateLocalDirectory() throws IOException {
        for (String name : remoteDirectory.listAll()) {
            copyFile(name, localDirectory, remoteDirectory);
        }
    }

    private void syncToRemote(List<? extends IndexCommit> commits) throws IOException {
        List<String> filesInPlay = new ArrayList<String>();
        for (IndexCommit commit : commits) {
            if (commit.isDeleted()) {
                continue;
            }
            Collection<String> fileNames = commit.getFileNames();
            for (String name : fileNames) {
                copyFile(name, localDirectory, remoteDirectory);
                filesInPlay.add(name);
            }
        }
        List<String> currentRemoteFiles = new ArrayList<String>(Arrays.asList(remoteDirectory.listAll()));
        currentRemoteFiles.removeAll(filesInPlay);
        for (String name : currentRemoteFiles) {
            remoteDirectory.deleteFile(name);
        }
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        onCommit(commits);
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        primaryIndexDeletionPolicy.onCommit(commits);
        syncToRemote(commits);
    }
}
