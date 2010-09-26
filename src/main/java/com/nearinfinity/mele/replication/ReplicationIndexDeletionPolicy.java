package com.nearinfinity.mele.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class ReplicationIndexDeletionPolicy implements IndexDeletionPolicy {

    private static final Log LOG = LogFactory.getLog(ReplicationIndexDeletionPolicy.class);
    private static final int BUFFER_SIZE = 65536;
    private static final long MEGA_BYTES_DOUBLE = 1024 * 1024;
    private int bufferSize = BUFFER_SIZE;
    
    private IndexDeletionPolicy primaryIndexDeletionPolicy;
    private Directory remoteDirectory;
    private Directory localDirectory;
    
    public ReplicationIndexDeletionPolicy(IndexDeletionPolicy policy, Directory localDirectory,
            Directory remoteDirectory) throws IOException {
        this(policy,localDirectory,remoteDirectory,BUFFER_SIZE);
    }

    public ReplicationIndexDeletionPolicy(IndexDeletionPolicy policy, Directory localDirectory,
            Directory remoteDirectory, int remoteDirBufferSize) throws IOException {
        this.primaryIndexDeletionPolicy = policy;
        this.localDirectory = localDirectory;
        this.remoteDirectory = remoteDirectory;
        this.bufferSize = remoteDirBufferSize;
        updateLocalDirectory();
    }

    private void updateLocalDirectory() throws IOException {
        for (String name : remoteDirectory.listAll()) {
            copyFile(name, localDirectory, remoteDirectory, bufferSize);
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
                copyFile(name, localDirectory, remoteDirectory, bufferSize);
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
    
    public static void copyFile(String name, Directory src, Directory dest, int bufferSize) throws IOException {
        if (src.fileExists(name) && dest.fileExists(name)) {
            if (src.fileLength(name) == dest.fileLength(name)) {
                // already there
                return;
            } else {
                dest.deleteFile(name);
            }
        }

        if (!src.fileExists(name) && dest.fileExists(name)) {
            dest.deleteFile(name);
            return;
        }

        LOG.info("copying file [" + name + "] from " + src + " to " + dest);

        byte[] buf = new byte[bufferSize];
        IndexOutput os = null;
        IndexInput is = null;
        long len = 0;
        long s = System.currentTimeMillis();
        try {
            // create file in dest directory
            os = dest.createOutput(name);
            // read current file
            is = src.openInput(name);
            len = is.length();
            // and copy to dest directory
            long readCount = 0;
            while (readCount < len) {
                int toRead = readCount + bufferSize > len ? (int) (len - readCount) : bufferSize;
                is.readBytes(buf, 0, toRead);
                os.writeBytes(buf, toRead);
                readCount += toRead;
            }
        } finally {
            // graceful cleanup
            try {
                if (os != null)
                    os.close();
            } finally {
                if (is != null)
                    is.close();
            }
        }
        double seconds = (System.currentTimeMillis() - s) / 1000.0;
        double megaBytesPerSecound = (len / MEGA_BYTES_DOUBLE) / seconds;
        LOG.info("Finished copying file [" + name + "] at rate [" + megaBytesPerSecound + " MB/s]");
    }
}
