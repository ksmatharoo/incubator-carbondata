package org.apache.carbondata.examples.sdk;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

public class LuceneTest {

  String indexDir = "/opt/lucene";
  Indexer indexer;
  Searcher searcher;
  Random random  = new Random();
  int cardinality = 100000;
  int row = 32000;
  int page = 500;
  int fields = 2;

  public static void main(String[] args) {
    LuceneTest tester;
    try {
      tester = new LuceneTest();
      tester.createIndex();
      tester.search(LuceneConstants.CONTENTS+"1:qwqwq* AND "+LuceneConstants.CONTENTS+"1:qwqwq*");
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  private void createIndex() throws IOException {
    indexer = new Indexer(indexDir);
    long startTime = System.currentTimeMillis();
    indexer.indexData(1, 1, 0);
    int size = 1000 * 1000 * 10;
    for (int i = 0; i < size; i++) {
      int nextInt = random.nextInt(cardinality);
//      indexer.indexData("qwqwq wm,qwew ssqs"+nextInt, nextInt, fields);
      indexer.indexDataBin("qwqwq wm,qwew ssqs"+nextInt, random.nextInt(row), random.nextInt(page), fields);
//      indexer.indexData(nextInt, nextInt, fields);
    }
    long endTime = System.currentTimeMillis();
    indexer.close(fields);
    System.out.println(" File indexed, time taken: "
        +(endTime-startTime)+" ms");
  }

  private void search(String searchQuery) throws IOException, ParseException {
    searcher = new Searcher(indexDir, fields);
    long startTime = System.currentTimeMillis();
    TopDocs hits = searcher.search(searchQuery);
    long endTime = System.currentTimeMillis();

    System.out.println(hits.totalHits +
        " documents found. Time :" + (endTime - startTime));

    for(ScoreDoc scoreDoc : hits.scoreDocs) {
      Document doc = searcher.getDocument(scoreDoc);
      System.out.println("File: "
          + doc.getField(LuceneConstants.FILE_NAME).binaryValue().length);
    }
  }
}
