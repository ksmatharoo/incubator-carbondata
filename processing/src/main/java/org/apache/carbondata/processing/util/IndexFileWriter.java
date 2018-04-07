package org.apache.carbondata.processing.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.block.BlockInfo;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileReaderImpl;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.AbstractDataFileFooterConverter;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.DataFileFooterConverterV3;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileWriter;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletBTreeIndex;
import org.apache.carbondata.format.BlockletIndex;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.IndexHeader;

public class IndexFileWriter {

  public static void main(String[] args) throws IOException {
//    File folder = new File("D:/vishal");
//    File[] carbonDataFiles = folder.listFiles(new FileFilter() {
//      @Override public boolean accept(File pathname) {
//        return pathname.getName().endsWith(".carbondata");
//      }
//    });
//    for (int i = 0; i < carbonDataFiles.length; i++) {
//      FileReader reader = new FileReaderImpl();
//      long offset = carbonDataFiles[i].length() - 8;
//      long actualOffset = reader.readLong(carbonDataFiles[i].getAbsolutePath(), offset);
//      TableBlockInfo info =
//          new TableBlockInfo(carbonDataFiles[i].getAbsolutePath(), actualOffset, "0", new String[0],
//              carbonDataFiles[i].length(), ColumnarFormatVersion.V3, null);
//      AbstractDataFileFooterConverter converter = new DataFileFooterConverterV3();
//      DataFileFooter dataFileFooter = converter.readDataFileFooter(info);
//      IndexHeader indexHeader = CarbonMetadataUtil
//          .getIndexHeader(dataFileFooter.getSegmentInfo().getColumnCardinality(),
//              getColumnSchemaListAndCardinality(dataFileFooter.getColumnInTable()), 0,
//              dataFileFooter.getSchemaUpdatedTimeStamp());
//      List<BlockIndex> blockIndex = new ArrayList<>();
//      List<BlockletInfo> blockletList = dataFileFooter.getBlockletList();
//      for (int k = 0; k < blockletList.size(); k++) {
//        BlockIndex index = new BlockIndex();
//        index.offset = actualOffset;
//        index.file_name = carbonDataFiles[k].getName();
//        index.setNum_rows(dataFileFooter.getNumberOfRows());
//        BlockletInfo3 blocletInfo3 = getBlocletInfo3(blockletList.get(k));
//        index.setBlocklet_info(blocletInfo3);
//        index.setBlock_index(getBlockletIndex(blockletList.get(k).getBlockletIndex()));
//        blockIndex.add(index);
//      }
//      String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(carbonDataFiles[i].getName());
//      long taskIdFromTaskNo = CarbonTablePath.DataFileUtil.getTaskIdFromTaskNo(taskNo);
//      String bucketNo = CarbonTablePath.DataFileUtil.getBucketNo(carbonDataFiles[i].getName());
//      int batchNoFromTaskNo = CarbonTablePath.DataFileUtil.getBatchNoFromTaskNo(taskNo);
//      String timeStampFromFileName =
//          CarbonTablePath.DataFileUtil.getTimeStampFromFileName(carbonDataFiles[i].getName());
//      String path = folder.getAbsolutePath() + File.separator + CarbonTablePath
//          .getCarbonIndexFileName(taskIdFromTaskNo, Integer.parseInt(bucketNo), batchNoFromTaskNo,
//              "" + timeStampFromFileName);
//      CarbonIndexFileWriter writer = new CarbonIndexFileWriter();
//      // open file
//      writer.openThriftWriter(path);
//      // write the header first
//      writer.writeThrift(indexHeader);
//      // write the indexes
//      for (BlockIndex block : blockIndex) {
//        writer.writeThrift(block);
//      }
//      writer.close();
//    }
        File file1 = new File("/opt/carbonstore/default/lineitem_batch_dict/Fact/Part0/Segment_1/0_batchno0-0-1522728211026.carbonindex");
        FileInputStream stream1 = new FileInputStream(file1);
        byte[] data1 = new byte[(int) file1.length()];
        stream1.read(data1);
        AbstractDataFileFooterConverter converter1 = new DataFileFooterConverterV3();
        List<DataFileFooter> indexInfo1 =
            converter1.getIndexInfo(file1.getAbsolutePath().replace("\\", "/"), data1);
        System.out.println();
  }

  public static BlockletInfo3 getBlocletInfo3(
      BlockletInfo blockletInfo) {
    List<Long> dimensionChunkOffsets = blockletInfo.getDimensionChunkOffsets();
    dimensionChunkOffsets.addAll(blockletInfo.getMeasureChunkOffsets());
    List<Integer> dimensionChunksLength = blockletInfo.getDimensionChunksLength();
    dimensionChunksLength.addAll(blockletInfo.getMeasureChunksLength());
    return new BlockletInfo3(blockletInfo.getNumberOfRows(), dimensionChunkOffsets,
        dimensionChunksLength, blockletInfo.getDimensionOffset(), blockletInfo.getMeasureOffsets(),
        blockletInfo.getNumberOfPages());
  }

  public static BlockletIndex getBlockletIndex(
      org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex info) {
    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();

    for (int i = 0; i < info.getMinMaxIndex().getMaxValues().length; i++) {
      blockletMinMaxIndex.addToMax_values(ByteBuffer.wrap(info.getMinMaxIndex().getMaxValues()[i]));
      blockletMinMaxIndex.addToMin_values(ByteBuffer.wrap(info.getMinMaxIndex().getMinValues()[i]));
    }
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex();
    blockletBTreeIndex.setStart_key(info.getBtreeIndex().getStartKey());
    blockletBTreeIndex.setEnd_key(info.getBtreeIndex().getEndKey());
    BlockletIndex blockletIndex = new BlockletIndex();
    blockletIndex.setMin_max_index(blockletMinMaxIndex);
    blockletIndex.setB_tree_index(blockletBTreeIndex);
    return blockletIndex;
  }

  public static List<org.apache.carbondata.format.ColumnSchema> getColumnSchemaListAndCardinality(
      List<ColumnSchema> wrapperColumnSchemaList) {
    List<org.apache.carbondata.format.ColumnSchema> columnSchemaList =
        new ArrayList<org.apache.carbondata.format.ColumnSchema>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    int counter = 0;
    for (int i = 0; i < wrapperColumnSchemaList.size(); i++) {
      columnSchemaList
          .add(schemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchemaList.get(i)));
    }
    return columnSchemaList;
  }
}
