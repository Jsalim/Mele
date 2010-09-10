package com.nearinfinity.mele.store.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import junit.framework.TestCase;

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

import com.nearinfinity.mele.store.MeleConfiguration;
import com.nearinfinity.mele.store.zookeeper.ZooKeeperFactory;

public class HdfsMeleTest extends TestCase {
	
	private FileSystem hdfsFileSystem;

	@Override
	protected void setUp() throws Exception {
		ZooKeeperFactory.create(new MeleConfiguration(), new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
		ZooKeeper zk = ZooKeeperFactory.getZooKeeper();
		DeleteZkNode.delete(zk, "/mele");
		rm(new File("./tmp"));
		hdfsFileSystem = FileSystem.getLocal(new Configuration());
	}

	public void testHdfsMele() throws Exception {
		HdfsMele mele1 = getHdfsMele("127.0.0.1","./tmp/tmp1");
		HdfsMele mele2 = getHdfsMele("127.0.0.2","./tmp/tmp2");
		HdfsMele mele3 = getHdfsMele("127.0.0.3","./tmp/tmp3");
		HdfsMele mele4 = getHdfsMele("127.0.0.4","./tmp/tmp4");
		HdfsMele mele5 = getHdfsMele("127.0.0.5","./tmp/tmp5");
		HdfsMele mele6 = getHdfsMele("127.0.0.6","./tmp/tmp6");
		HdfsMele mele7 = getHdfsMele("127.0.0.7","./tmp/tmp7");
		HdfsMele mele8 = getHdfsMele("127.0.0.8","./tmp/tmp8");
		HdfsMele mele9 = getHdfsMele("127.0.0.9","./tmp/tmp9");
		HdfsMele mele10 = getHdfsMele("127.0.0.10","./tmp/tmp10");
		mele1.createDirectoryCluster("test");
		populate(mele1, "test", "test-1");
		populate(mele2, "test", "test-2");
		populate(mele3, "test", "test-3");
		populate(mele4, "test", "test-4");
		populate(mele5, "test", "test-5");
		populate(mele6, "test", "test-6");
		populate(mele7, "test", "test-7");
		populate(mele8, "test", "test-8");
		populate(mele9, "test", "test-9");
		populate(mele10, "test", "test-10");
	}
	
	private void populate(HdfsMele mele, String cluster, String dir) throws Exception {
		mele.createDirectory(cluster, dir);
		Directory directory = mele.open(cluster, dir);
		populate(directory,mele.getIndexDeletionPolicy(cluster, dir));
	}

	private void populate(Directory directory, IndexDeletionPolicy indexDeletionPolicy) throws Exception {
		IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), indexDeletionPolicy, MaxFieldLength.UNLIMITED);
		for (int i = 0; i < 10000; i++) {
			writer.addDocument(genDoc());
		}
		writer.close();
	}

	private Document genDoc() {
		Document document = new Document();
		document.add(new Field("id",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
		return document;
	}

	private HdfsMele getHdfsMele(String localhost,String dir) throws IOException {
		File file = new File("./tmp/mele");
		file.mkdirs();
		MeleConfiguration conf = new MeleConfiguration();
		conf.setBaseHdfsPath(file.getAbsolutePath());
		conf.setHdfsFileSystem(hdfsFileSystem);
		conf.setLocalReplicationPathList(Arrays.asList(dir));
		conf.setUsingHdfs(true);
		return new HdfsMele(conf);
	}
	
	private void rm(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				rm(f);
			}
		}
		file.delete();
	}
}
