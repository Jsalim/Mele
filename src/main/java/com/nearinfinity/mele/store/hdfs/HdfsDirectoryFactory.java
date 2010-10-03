package com.nearinfinity.mele.store.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.nearinfinity.mele.MeleConfiguration;
import com.nearinfinity.mele.MeleDirectoryFactory;

public class HdfsDirectoryFactory implements MeleDirectoryFactory {
    
    private String baseHdfsPath;
    private FileSystem hdfsFileSystem;

    public HdfsDirectoryFactory(MeleConfiguration configuration, FileSystem hdfsFileSystem) throws IOException {
        this.hdfsFileSystem = hdfsFileSystem;
        this.baseHdfsPath = configuration.getBaseHdfsPath();
    }

    public Directory getDirectory(FSDirectory localDir, String directoryCluster, String directoryName) throws IOException {
        Path hdfsDirPath = new Path(baseHdfsPath, directoryCluster);
        return new HdfsDirectory(new Path(hdfsDirPath, directoryName), hdfsFileSystem);
    }
}
