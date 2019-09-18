package org.apache.carbondata.examples.sdk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene62.Lucene62Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.roaringbitmap.RoaringBitmap;

public class Indexer {

  private IndexWriter writer;

  private Map<String, Map<Integer, RoaringBitmap>> map = new HashMap<>();
  private Compressor compressor = CompressorFactory.getInstance().getCompressor();

  public Indexer(String indexDirectoryPath) throws IOException {
    //this directory will contain the indexes
    Directory indexDirectory =
        FSDirectory.open(new File(indexDirectoryPath).toPath());
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
    indexWriterConfig
        .setCodec(new Lucene62Codec(Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION));
    //create the indexer
    writer = new IndexWriter(indexDirectory,indexWriterConfig);
  }

  public void close(int fields) throws CorruptIndexException, IOException {
    finishStore(fields);
    writer.close();
  }

  private Document getDocument(String key, int value, int fields) throws IOException {
    Document document = new Document();

    for (int i = 0; i < fields; i++) {
      //index file contents
      document.add(new StringField(LuceneConstants.CONTENTS+i, key, Field.Store.NO));
    }
    //index file name
//    document.add(new IntPoint(LuceneConstants.FILE_NAME, new int[]{value}));
    document.add(new StoredField(LuceneConstants.FILE_NAME, value));

    return document;
  }

  private Document getDocument(int key, int value, int fields) throws IOException {
    Document document = new Document();

    //index file contents
    Field contentField = new IntPoint(LuceneConstants.CONTENTS, new int[]{value});
    //index file name
    //    Field fileNameField = new IntPoint(LuceneConstants.FILE_NAME, new int[]{value});
    document.add(new StoredField(LuceneConstants.FILE_NAME, value));

    document.add(contentField);
    //    document.add(fileNameField);

    return document;
  }

  private Document getDocument(String key, Map<Integer, RoaringBitmap> value, int fields) throws IOException {

    Document document = new Document();

    //index file contents
    for (int i = 0; i < fields; i++) {
      //index file contents
      document.add(new StringField(LuceneConstants.CONTENTS+i, key, Field.Store.NO));
    }

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream stream1 = new DataOutputStream(stream);
    stream1.writeInt(value.size());
    for (Map.Entry<Integer, RoaringBitmap> entry : value.entrySet()) {
      stream1.writeShort(entry.getKey());
      entry.getValue().serialize(stream1);
    }
    //index file name
    //    Field fileNameField = new IntPoint(LuceneConstants.FILE_NAME, new int[]{value});
    stream1.close();
    document.add(new StoredField(LuceneConstants.FILE_NAME, compressor.compressByte(stream.toByteArray())));

    return document;
  }

  public void indexData(String key, int value, int fields) throws IOException {

    Document document = getDocument(key, value, fields);
    writer.addDocument(document);
  }

  public void indexDataBin(String key, int value, int pageId, int fields) throws IOException {
    addToCache(key, value, pageId);
    checkIfStore(fields);
  }

  private void addToCache(String key, int value, int pageId) {
    Map<Integer, RoaringBitmap> setMap = map.get(key);
    if (setMap == null) {
      setMap = new HashMap<>();
      map.put(key, setMap);
    }
    RoaringBitmap bitSet = setMap.get(pageId);
    if (bitSet == null) {
      bitSet = new RoaringBitmap();
      setMap.put(pageId, bitSet);
    }
    bitSet.add(value);
  }

  private void checkIfStore(int fields) throws IOException {
    if (map.size() > 100000) {
      finishStore(fields);
    }
  }

  private void finishStore(int fields) throws IOException {
    for (Map.Entry<String, Map<Integer, RoaringBitmap>> entry : map.entrySet()) {
      Document document = getDocument(entry.getKey(), entry.getValue(), fields);
      writer.addDocument(document);
    }
    map.clear();
  }

  public void indexData(int key, int value, int fields) throws IOException {
    Document document = getDocument(key, value, fields);
    writer.addDocument(document);
  }

}