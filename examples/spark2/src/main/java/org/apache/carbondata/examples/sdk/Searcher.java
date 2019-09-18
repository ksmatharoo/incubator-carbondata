package org.apache.carbondata.examples.sdk;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class Searcher {
  IndexSearcher indexSearcher;
  QueryParser queryParser;
  Query query;

  public Searcher(String indexDirectoryPath, int size)
      throws IOException {
    Directory indexDirectory =
        FSDirectory.open(new File(indexDirectoryPath).toPath());
    indexSearcher = new IndexSearcher(DirectoryReader.open(indexDirectory));
    String[] strings = new String[size];
    for (int i = 0; i < size; i++) {
      strings[i] = LuceneConstants.CONTENTS+i;
    }
    queryParser = new MultiFieldQueryParser(strings, new StandardAnalyzer());
  }

  public TopDocs search( String searchQuery)
      throws IOException, ParseException {
    query = queryParser.parse(searchQuery);
    return indexSearcher.search(query, LuceneConstants.MAX_SEARCH);
  }

  public Document getDocument(ScoreDoc scoreDoc)
      throws CorruptIndexException, IOException {
    return indexSearcher.doc(scoreDoc.doc);
  }
}
