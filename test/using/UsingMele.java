package using;

import java.io.File;
import java.util.Random;
import java.util.UUID;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.nearinfinity.mele.Mele;

public class UsingMele {

	public static void main(String[] args) throws Exception {
//		simpleTest();
		Mele mele = Mele.getMele();
		Directory directory = mele.open("test", "test1");
		
		Directory.copy(directory,FSDirectory.open(new File("./index")),true);
		
//		IndexDeletionPolicy indexDeletionPolicy = mele.getIndexDeletionPolicy("test", "test1");
//		IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT),
//				indexDeletionPolicy, MaxFieldLength.UNLIMITED);
//		writer.setUseCompoundFile(false);
//		for (int j = 0; j < 10; j++) {
//			int count = 0;
//			int max = 10000;
//			long s = System.currentTimeMillis();
//			for (int i = 0; i < 1000000; i++) {
//				if (count >= max) {
//					double seconds = (System.currentTimeMillis() - s) / 1000.0;
//					double rate = i / seconds;
//					System.out.println(i + " at " + rate);
//					count = 0;
//				}
//				writer.addDocument(genDoc());
//				count++;
//			}
//			writer.commit();
//			System.out.println("Done....");
//		}
//		writer.close();

		IndexSearcher searcher = new IndexSearcher(directory);
		double totalTime = 0;
		long hits = 0;
		int pass = 1000;
		for (int i = 0; i < pass; i++) {
			long st = System.nanoTime();
			TopDocs topDocs = searcher.search(new TermQuery(new Term("f3", "value" + random.nextInt(10000))), 10);
			long et = System.nanoTime();
			totalTime += (et - st);
			hits += topDocs.totalHits;
		}
		System.out.println(hits + " " + (totalTime / 1000000)/pass);
	}

//	private static void simpleTest() throws Exception {
//		Mele mele = Mele.getMele();
//		Directory directory = mele.open("test", "test1");
//		IndexDeletionPolicy indexDeletionPolicy = mele.getIndexDeletionPolicy("test", "test1");
//		IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT),
//				indexDeletionPolicy, MaxFieldLength.UNLIMITED);
//		writer.addDocument(genDoc());
//		writer.close();
//
//		IndexSearcher searcher = new IndexSearcher(directory);
//		TopDocs topDocs = searcher.search(new TermQuery(new Term("f", "v")), 10);
//		System.out.println(topDocs.totalHits);
//
//	}

	private static Random random = new Random(1);

	private static Document genDoc() {
		Document doc = new Document();
		doc.add(new Field("f", "v", Store.YES, Index.ANALYZED_NO_NORMS));
		doc.add(new Field("f0", "value" + random.nextInt(10), Store.YES, Index.ANALYZED_NO_NORMS));
		doc.add(new Field("f1", "value" + random.nextInt(100), Store.YES, Index.ANALYZED_NO_NORMS));
		doc.add(new Field("f2", "value" + random.nextInt(1000), Store.YES, Index.ANALYZED_NO_NORMS));
		doc.add(new Field("f3", "value" + random.nextInt(10000), Store.YES, Index.ANALYZED_NO_NORMS));
		doc.add(new Field("f4", "value" + random.nextInt(100000), Store.YES, Index.ANALYZED_NO_NORMS));
		doc.add(new Field("f5", "value" + random.nextInt(1000000), Store.YES, Index.ANALYZED_NO_NORMS));
		doc.add(new Field("f6", "value" + random.nextInt(10000000), Store.YES, Index.ANALYZED_NO_NORMS));
		doc.add(new Field("f7", "value" + random.nextInt(100000000), Store.YES, Index.ANALYZED_NO_NORMS));
		doc.add(new Field("f8", "value" + random.nextInt(1000000000), Store.YES, Index.ANALYZED_NO_NORMS));
		doc.add(new Field("id", UUID.randomUUID().toString(), Store.YES, Index.ANALYZED_NO_NORMS));
		return doc;
	}

}
