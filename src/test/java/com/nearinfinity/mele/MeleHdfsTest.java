package com.nearinfinity.mele;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nearinfinity.mele.store.hdfs.HdfsDirectory;
import com.nearinfinity.mele.store.hdfs.HdfsDirectoryFactory;
import com.nearinfinity.mele.util.ZkUtils;

public class MeleHdfsTest {

    private static String dataDirectoryName = "target/zookeeper-data";
    private static File dataDirectory;
    private static ZooKeeper zk;
    private static Thread zkDaemon;

    private static final String ZK_CONNECTION_STRING = "localhost:3181";
    private static final int _1000 = 1000;

    private FileSystem hdfsFileSystem;
    private File meleFile;
    private String directoryCluster = "test";

    @BeforeClass
    public static void setUpOnce() throws Exception {
        dataDirectory = new File(dataDirectoryName);
        if (dataDirectory.exists()) {
            rm(dataDirectory);
        }
        MeleConfiguration config = new MeleConfiguration();
        config.setZooKeeperConnectionString(ZK_CONNECTION_STRING);
        zk = new ZooKeeper(config.getZooKeeperConnectionString(),config.getZooKeeperSessionTimeout(),new Watcher(){
            @Override
            public void process(WatchedEvent event) {
                
            }
        });
        startEmbeddedZooKeeperThread();
        waitForZooKeeperToStart();
    }
    
    @AfterClass
    public static void tearDownOnceComplete() throws InterruptedException {
        zk.close();
        zkDaemon.interrupt();
    }

    private static void startEmbeddedZooKeeperThread() {
        zkDaemon = new Thread(new Runnable() {
            @Override
            public void run() {
                QuorumPeerMain.main(new String[]{ "src/test/resources/zoo-embeddable.cfg" });
            }
        });
        zkDaemon.setDaemon(true);
        zkDaemon.start();
    }

    private static void waitForZooKeeperToStart() throws IOException, InterruptedException {
        while (zk.getState() != ZooKeeper.States.CONNECTED) {
            Thread.sleep(100);
        }
    }

    @Before
    public void setUp() throws Exception {
        meleFile = new File(dataDirectory, "mele");
        meleFile.mkdirs();
        ZkUtils.deleteAnyVersion(zk, "/mele");
        rm(new File(dataDirectory, "tmp"));
        hdfsFileSystem = FileSystem.getLocal(new Configuration());
    }

    @Test
    public void testMeleWithEmptyRemoteDirectories() throws Exception {
        List<MeleBase> meles = new ArrayList<MeleBase>();
        for (int i = 0; i < 5; i++) {
            meles.add(newMele("tmp" + i));
        }
        meles.get(0).createDirectoryCluster(directoryCluster);

        populateMeles(meles);
        assertLocalAndRemoteFilesystemAreTheSame(meles);
        assertNumberOfDocumentsInLuceneDirectory(meles, _1000);
    }

    @Test
    public void testMeleWithPopulatedRemoteDirectories() throws Exception {
        int size = 5;
        populateHdfsDirs(size);
        List<MeleBase> meles = new ArrayList<MeleBase>();
        for (int i = 0; i < size; i++) {
            meles.add(newMele("tmp" + i));
        }
        meles.get(0).createDirectoryCluster(directoryCluster);

        populateMeles(meles);
        assertLocalAndRemoteFilesystemAreTheSame(meles);
        assertNumberOfDocumentsInLuceneDirectory(meles, _1000 * 2);
    }

    private void assertNumberOfDocumentsInLuceneDirectory(List<MeleBase> meles, int expectedNumDocs)
            throws IOException {
        for (int i = 0; i < meles.size(); i++) {
            Mele mele = meles.get(i);
            Directory directory = mele.open(directoryCluster, "test-" + i);
            assertEquals(expectedNumDocs, IndexReader.open(directory).numDocs());
        }
    }

    private void assertLocalAndRemoteFilesystemAreTheSame(List<MeleBase> meles) {
        for (int i = 0; i < meles.size(); i++) {
            assertFiles(new File(dataDirectoryName + "/tmp" + i + "/test/test-" + i),
                    new File(dataDirectoryName + "/mele/test/test-" + i));
        }
    }

    private void populateMeles(List<MeleBase> meles) throws Exception {
        for (int i = 0; i < meles.size(); i++) {
            Mele mele = meles.get(i);
            populate(mele, directoryCluster, "test-" + i);
        }
    }

    private void populateHdfsDirs(int numberOfDirs) throws Exception {
        for (int i = 0; i < numberOfDirs; i++) {
            Directory dir = new RAMDirectory();
            populate(dir, new KeepOnlyLastCommitDeletionPolicy());
            Path hdfsDirPath = new Path(meleFile.getAbsolutePath(), directoryCluster);
            HdfsDirectory directory = new HdfsDirectory(new Path(hdfsDirPath, "test-" + i), hdfsFileSystem);
            Directory.copy(dir, directory, true);
        }
    }

    private void assertFiles(File localCopy, File remoteCopy) {
        assertTrue(localCopy.exists());
        assertTrue(remoteCopy.exists());
        assertTrue(localCopy.isDirectory());
        assertTrue(remoteCopy.isDirectory());
        SortedSet<String> localFiles = getFiles(localCopy);
        SortedSet<String> remoteFiles = getFiles(remoteCopy);
        assertEquals(localFiles, remoteFiles);
        for (String name : localFiles) {
            File lf = new File(localCopy, name);
            File rf = new File(remoteCopy, name);
            assertEquals(lf.length(), rf.length());
        }
    }

    private SortedSet<String> getFiles(File dir) {
        TreeSet<String> result = new TreeSet<String>();
        for (File f : dir.listFiles()) {
            String name = f.getName();
            if (!(name.equals("segments.gen") || name.endsWith(".crc"))) {
                result.add(name);
            }
        }
        return result;
    }

    private void populate(Mele mele, String cluster, String dir) throws Exception {
        mele.createDirectory(cluster, dir);
        Directory directory = mele.open(cluster, dir);
        populate(directory, mele.getIndexDeletionPolicy(cluster, dir));
    }

    private void populate(Directory directory, IndexDeletionPolicy indexDeletionPolicy) throws Exception {
        IndexWriter writer =
                new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_30), indexDeletionPolicy,
                        MaxFieldLength.UNLIMITED);
        for (int i = 0; i < _1000; i++) {
            writer.addDocument(genDoc());
        }
        writer.close();
    }

    private Document genDoc() {
        Document document = new Document();
        document.add(new Field("id", UUID.randomUUID().toString(), Store.YES, Index.ANALYZED_NO_NORMS));
        return document;
    }

    private MeleBase newMele(String dir) throws IOException {
        File fullDir = new File(dataDirectory, dir);
        MeleConfiguration conf = new MeleConfiguration();
        conf.setZooKeeperConnectionString(ZK_CONNECTION_STRING);
        conf.setBaseHdfsPath(meleFile.getAbsolutePath());
        conf.setLocalReplicationPathList(Arrays.asList(fullDir.getPath()));
        return new MeleBase(new HdfsDirectoryFactory(conf, hdfsFileSystem),conf,zk);
    }

    private static void rm(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
    }
}
