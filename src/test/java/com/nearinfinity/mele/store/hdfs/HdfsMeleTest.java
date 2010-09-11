package com.nearinfinity.mele.store.hdfs;

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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nearinfinity.mele.MeleConfiguration;
import com.nearinfinity.mele.store.zookeeper.ZooKeeperFactory;
import static junit.framework.TestCase.*;

public class HdfsMeleTest {

    private static File dataDirectory;
    private FileSystem hdfsFileSystem;

    @BeforeClass
    public static void setUpOnce() throws Exception {
        dataDirectory = new File("target/zookeeper-data");
        if (dataDirectory.exists()) {
            rm(dataDirectory);
        }

        Thread zkDaemon = new Thread(new Runnable() {
            @Override
            public void run() {
                QuorumPeerMain.main(new String[]{ "src/test/resources/zoo-embeddable.cfg" });
            }
        });
        zkDaemon.setDaemon(true);
        zkDaemon.start();

        // TODO Need better way to wait for embedded server start than this sleep!
        Thread.sleep(5000);
    }

    @Before
    public void setUp() throws Exception {
        ZooKeeperFactory.create(new MeleConfiguration(), new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
        ZooKeeper zk = ZooKeeperFactory.getZooKeeper();
        DeleteZkNode.delete(zk, "/mele");
        rm(new File(dataDirectory, "tmp"));
        hdfsFileSystem = FileSystem.getLocal(new Configuration());
    }

    @Test
    public void testHdfsMele() throws Exception {
    	List<HdfsMele> meles = new ArrayList<HdfsMele>();
    	for (int i = 0; i < 5; i++) {
    		meles.add(getHdfsMele("tmp" + i));
    	}
        meles.get(0).createDirectoryCluster("test");
        
        for (int i = 0; i < meles.size(); i++) {
        	HdfsMele hdfsMele = meles.get(i);
        	populate(hdfsMele, "test", "test-" + i);
        }
        
        for (int i = 0; i < meles.size(); i++) {
        	assertFiles(new File("target/zookeeper-data/tmp" + i + "/test/test-" + i),
        			new File("target/zookeeper-data/mele/test/test-" + i));
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
    		File lf = new File(localCopy,name);
    		File rf = new File(remoteCopy,name);
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

	private void populate(HdfsMele mele, String cluster, String dir) throws Exception {
        mele.createDirectory(cluster, dir);
        Directory directory = mele.open(cluster, dir);
        populate(directory, mele.getIndexDeletionPolicy(cluster, dir));
    }

    private void populate(Directory directory, IndexDeletionPolicy indexDeletionPolicy) throws Exception {
        IndexWriter writer =
                new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_30), indexDeletionPolicy,
                        MaxFieldLength.UNLIMITED);
        for (int i = 0; i < 10000; i++) {
            writer.addDocument(genDoc());
        }
        writer.close();
    }

    private Document genDoc() {
        Document document = new Document();
        document.add(new Field("id", UUID.randomUUID().toString(), Store.YES, Index.ANALYZED_NO_NORMS));
        return document;
    }

    private HdfsMele getHdfsMele(String dir) throws IOException {
        File file = new File(dataDirectory, "mele");
        file.mkdirs();
        MeleConfiguration conf = new MeleConfiguration();
        conf.setBaseHdfsPath(file.getAbsolutePath());
        conf.setHdfsFileSystem(hdfsFileSystem);
        File fullDir = new File(dataDirectory, dir);
        conf.setLocalReplicationPathList(Arrays.asList(fullDir.getPath()));
        conf.setUsingHdfs(true);
        return new HdfsMele(conf);
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
