package com.nearinfinity.mele;

import java.io.IOException;

import org.apache.lucene.store.Directory;

public interface MeleDirectoryFactory {

    Directory getDirectory(String directoryCluster, String directoryName) throws IOException;

}
