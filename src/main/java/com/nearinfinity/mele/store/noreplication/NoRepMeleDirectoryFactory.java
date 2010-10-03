package com.nearinfinity.mele.store.noreplication;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.nearinfinity.mele.MeleDirectoryFactory;

public class NoRepMeleDirectoryFactory implements MeleDirectoryFactory {

    @Override
    public Directory getDirectory(FSDirectory localDir, String directoryCluster, String directoryName) throws IOException {
        return localDir;
    }
    
}
