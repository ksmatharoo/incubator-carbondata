package org.apache.carbondata.processing.util;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v3.CompressedDimensionChunkFileBasedReaderV3;
import org.apache.carbondata.core.datastore.chunk.reader.measure.v3.CompressedMeasureChunkFileBasedReaderV3;
import org.apache.carbondata.core.datastore.compression.SnappyCompressor;
import org.apache.carbondata.core.datastore.impl.FileReaderImpl;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonDictionaryReaderImpl;
import org.apache.carbondata.core.util.AbstractDataFileFooterConverter;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CompressedColumnPageVo;
import org.apache.carbondata.core.util.DataFileFooterConverterV3;

public class LocalDictionaryGenerator {

  private static List<byte[]> dictionary;

  private static String outDir;

  public static void main(String[] args) throws IOException {
    File file = new File("/opt/carbonstore/default/lineitem_batch_dict");
    outDir = file.getAbsolutePath()+ File.separator + "Fact1";
    File out = new File(outDir);
    out.mkdirs();

    File f = new File(file.getAbsolutePath() + "/Fact/Part0/Segment_1");
    File[] carbonDataFiles= f.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return pathname.getName().endsWith(".carbondata");
      }
    });
    main1(file.getAbsolutePath(), carbonDataFiles);
  }
  public static void main1(String actualPath, File[] carbondataFiles) throws IOException {
    for (int i = 0; i < carbondataFiles.length; i++) {
      long offset = carbondataFiles[i].length();
      long actualOffset =
          new FileReaderImpl().readLong(carbondataFiles[i].getAbsolutePath(), offset - 8);
      TableBlockInfo blockInfo =
          new TableBlockInfo(carbondataFiles[i].getAbsolutePath(), actualOffset, "0", new String[0],
              carbondataFiles[i].length(), ColumnarFormatVersion.V3, null);
      AbstractDataFileFooterConverter converter = new DataFileFooterConverterV3();
      DataFileFooter dataFileFooter = converter.readDataFileFooter(blockInfo);
      List<ColumnSchema> columnInTable = dataFileFooter.getColumnInTable();
      if (null == dictionary) {
        dictionary = getColDictionaryMap(columnInTable, actualPath + File.separator + "Metadata");
      }

      List<ColumnSchema> dimColSchema1 = new ArrayList<>();
      for (ColumnSchema cols : columnInTable) {
        if (cols.isDimensionColumn()) {
          dimColSchema1.add(cols);
        }
      }
      List<Integer> dimColSchema = new ArrayList<>();
      for (ColumnSchema cols : dimColSchema1) {
        if (cols.isDimensionColumn() && CarbonUtil
            .hasEncoding(cols.getEncodingList(), Encoding.DICTIONARY) && !CarbonUtil
            .hasEncoding(cols.getEncodingList(), Encoding.DIRECT_DICTIONARY)) {
          dimColSchema.add(1);
        } else {
          dimColSchema.add(-1);
        }
      }
      BlockletInfo blockletList = dataFileFooter.getBlockletList().get(0);
      FileReaderImpl fileReader = new FileReaderImpl();
      CompressedDimensionChunkFileBasedReaderV3 dimensionColumnChunkReader =
          (CompressedDimensionChunkFileBasedReaderV3) CarbonDataReaderFactory.getInstance()
              .getDimensionColumnChunkReader(ColumnarFormatVersion.V3, blockletList,
                  dataFileFooter.getSegmentInfo().getColumnCardinality(),
                  carbondataFiles[i].getAbsolutePath(), false);
      CompressedMeasureChunkFileBasedReaderV3 measureColumnChunkReader =
          (CompressedMeasureChunkFileBasedReaderV3) CarbonDataReaderFactory.getInstance()
              .getMeasureColumnChunkReader(ColumnarFormatVersion.V3, blockletList,
                  carbondataFiles[i].getAbsolutePath(), false);
      CompressedColumnPageVo[] dimensionColumnPages =
          dimensionColumnChunkReader.readRawDimensionChunksInGroup(fileReader);
      int counter = 0;
      for (int k = 0; k < dimensionColumnPages.length; k++) {
        if(dimColSchema.get(k)==1) {
          dimensionColumnPages[k].dataChunk3.localDictionary = new ArrayList<>();
          dimensionColumnPages[k].dataChunk3.localDictionary.add(ByteBuffer.wrap(dictionary.get(counter++)));
        }
      }
      CompressedColumnPageVo[] measureColumnPages =
          measureColumnChunkReader.readRawMeasureChunksInGroup(fileReader);
      CarbonDataFileWriter writer =
          new CarbonDataFileWriter(outDir + File.separator + carbondataFiles[i].getName(),
              dataFileFooter);
      writer.writeData(dimensionColumnPages, measureColumnPages);
    }
  }

  private static List<byte[]> getColDictionaryMap(List<ColumnSchema> columnSchemas,
      String dictionaryPath) throws IOException {
    List<ColumnSchema> dimColSchema = new ArrayList<>();
    for (ColumnSchema cols : columnSchemas) {
      if (cols.isDimensionColumn() && CarbonUtil
          .hasEncoding(cols.getEncodingList(), Encoding.DICTIONARY) && !CarbonUtil
          .hasEncoding(cols.getEncodingList(), Encoding.DIRECT_DICTIONARY)) {
        dimColSchema.add(cols);
      }
    }
    List<byte[]> dicData = new ArrayList<>();
    SnappyCompressor compressor = new SnappyCompressor();
    for (ColumnSchema dic : dimColSchema) {
      List<byte[]> read =
          new CarbonDictionaryReaderImpl(dictionaryPath, dic.getColumnUniqueId()).read();
      dicData.add(compressor.compressByte(convertToLV(read)));
    }
    return dicData;
  }

  private static byte[] convertToLV(List<byte[]> dicData) {
    int totalSize = dicData.size() * 2;
    for (byte[] dicVal : dicData) {
      totalSize += dicVal.length;
    }
    ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
    for (byte[] dicVal : dicData) {
      byteBuffer.putShort((short) dicVal.length);
      byteBuffer.put(dicVal);
    }
    byteBuffer.rewind();
    return byteBuffer.array();
  }
}