package com.nearinfinity.mele;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public interface MeleDirectoryFactory {

    Directory getDirectory(FSDirectory localDir, String directoryCluster, String directoryName) throws IOException;

}
