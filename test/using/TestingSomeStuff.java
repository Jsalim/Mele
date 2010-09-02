package using;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;


public class TestingSomeStuff {

	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
//		IndexReader reader = IndexReader.open(FSDirectory.open(new File("./tmp-index")));
//		System.out.println(reader.numDocs());
//		if (true) {
//			return;
//		}
		IndexDeletionPolicy deletionPolicy = new IndexDeletionPolicy() {
			@Override
			public void onInit(List<? extends IndexCommit> commits) throws IOException {
				
			}
			@Override
			public void onCommit(List<? extends IndexCommit> commits) throws IOException {
				System.out.println("onCommit " + commits);
			}
		};
		NIOFSDirectory directory = new NIOFSDirectory(new File("./tmp-index")) {
			@Override
			public void sync(String name) throws IOException {
				super.sync(name);
				System.out.println("Sync " + name);
			}
		};
		IndexWriter writer = new IndexWriter(directory, 
				new StandardAnalyzer(Version.LUCENE_CURRENT), 
				deletionPolicy, MaxFieldLength.UNLIMITED);
		System.out.println("Open");
		writer.addDocument(genDoc());
		System.out.println("Before Commit");
		writer.commit();
		System.out.println("After Commit");
		writer.close();
	}

	private static Document genDoc() {
		Document document = new Document();
		document.add(new Field("name","value",
				Store.YES, Index.ANALYZED_NO_NORMS));
		return document;
	}

}
