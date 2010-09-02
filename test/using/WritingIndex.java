package using;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.mele.store.hdfs.ReplicatedDirectory;

public class WritingIndex {

	private static final File PATH1 = new File("./dir1");
	private static final File GOLD_COPY = new File("./remote-gold-copy");
	private static final File PATH3 = new File("./dir3");

	public static void main(String[] args) throws IOException, InterruptedException {
		rm(PATH1,GOLD_COPY,PATH3);
		
		generateIndex(GOLD_COPY);
		
		PATH1.mkdirs();
		GOLD_COPY.mkdirs();
		PATH3.mkdirs();
		
		Directory dir1 = FSDirectory.open(PATH1);
		Directory remote = FSDirectory.open(GOLD_COPY);
		ReplicatedDirectory replicatedDirectory = new ReplicatedDirectory(dir1, remote, true);

		IndexDeletionPolicy idp = new KeepOnlyLastCommitDeletionPolicy();
		IndexWriter writer = new IndexWriter(replicatedDirectory, new StandardAnalyzer(Version.LUCENE_CURRENT), idp, MaxFieldLength.UNLIMITED);
		writer.setUseCompoundFile(false);

		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Directory dir3 = FSDirectory.open(PATH3);
					Directory remote = FSDirectory.open(GOLD_COPY);
					ReplicatedDirectory replicatedDirectory = new ReplicatedDirectory(dir3, remote, false);
					while (!IndexReader.indexExists(replicatedDirectory)) {
						System.out.println("Waiting for index to appear...");
						Thread.sleep(1000);
					}
					IndexReader reader = IndexReader.open(replicatedDirectory);
					while (true) {
						if (!reader.isCurrent()) {
							IndexReader reopen = reader.reopen();
							reader.close();
							reader = reopen;
						}
						System.out.println("Count [" + reader.numDocs() + "]");
						Thread.sleep(250);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		thread.setName("reader thread");
		thread.start();
		
		for (int j = 0; j < 10; j++) {
			for (int i = 0; i < 10000; i++) {
				writer.addDocument(genDoc());
			}
			writer.commit();
			Thread.sleep(1000);
		}
		writer.close();
	}

	private static void generateIndex(File goldCopy) throws IOException {
		IndexWriter writer = new IndexWriter(FSDirectory.open(goldCopy), new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		writer.setUseCompoundFile(false);
		for (int i = 0; i < 12341; i++) {
			writer.addDocument(genDoc());
		}
		writer.close();
	}

	private static void rm(File... paths) {
		for (File p : paths) {
			rmInternal(p);
		}
	}
	
	private static void rmInternal(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				rm(f);
			}
		}
		file.delete();
	}

	private static Document genDoc() {
		Document document = new Document();
		document.add(new Field("id",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
		return document;
	}

}
