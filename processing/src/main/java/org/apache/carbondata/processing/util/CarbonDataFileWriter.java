package org.apache.carbondata.processing.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CompressedColumnPageVo;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileWriter;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletBTreeIndex;
import org.apache.carbondata.format.BlockletIndex;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.ColumnSchema;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.format.IndexHeader;

public class CarbonDataFileWriter {

  List<ColumnSchema> thriftColumnSchemaList;

  long schemaupdatedTime;

  FileChannel fileChannel;

  String filePath;

  long currentOffsetInFile;

  DataFileFooter footer;

  public CarbonDataFileWriter(String filePath, DataFileFooter footer) throws IOException {
    this.schemaupdatedTime = footer.getSchemaUpdatedTimeStamp();
    this.fileChannel = new FileOutputStream(filePath).getChannel();
    this.filePath = filePath;
    this.footer = footer;
    this.thriftColumnSchemaList = getColumnSchemaListAndCardinality(footer.getColumnInTable());
    writeHeaderToFile();
  }

  public void writeData(CompressedColumnPageVo[] dimensionVo, CompressedColumnPageVo[] msrVo)
      throws IOException {
    long offset = currentOffsetInFile;
    List<Long> currentDataChunksOffset = new ArrayList<>();
    List<Integer> currentDataChunksLength = new ArrayList<>();
    long dimensionOffset = 0;
    long measureOffset = 0;
    ByteBuffer buffer = null;
    for (int i = 0; i < dimensionVo.length; i++) {
      byte[] dataChunkBytes = CarbonUtil.getByteArray(dimensionVo[i].dataChunk3);
      currentDataChunksOffset.add(offset);
      currentDataChunksLength.add(dataChunkBytes.length);
      buffer = ByteBuffer.wrap(dataChunkBytes);
      currentOffsetInFile += fileChannel.write(buffer);
      offset += dataChunkBytes.length;
      buffer = ByteBuffer.wrap(dimensionVo[i].data);
      int bufferSize = buffer.limit();
      currentOffsetInFile += fileChannel.write(buffer);
      offset += bufferSize;
    }
    dimensionOffset = offset;
    for (int i = 0; i < msrVo.length; i++) {
      byte[] dataChunkBytes = CarbonUtil.getByteArray(msrVo[i].dataChunk3);
      currentDataChunksOffset.add(offset);
      currentDataChunksLength.add(dataChunkBytes.length);
      buffer = ByteBuffer.wrap(dataChunkBytes);
      currentOffsetInFile += fileChannel.write(buffer);
      offset += dataChunkBytes.length;
      buffer = ByteBuffer.wrap(msrVo[i].data);
      int bufferSize = buffer.limit();
      currentOffsetInFile += fileChannel.write(buffer);
      offset += bufferSize;
    }
    measureOffset = offset;
    BlockletInfo3 blockletInfo3 =
        new BlockletInfo3((int) footer.getNumberOfRows(), currentDataChunksOffset,
            currentDataChunksLength, dimensionOffset, measureOffset,
            footer.getBlockletList().get(0).getNumberOfPages());
    List<BlockletInfo3> blockletInfo3List = new ArrayList<>();
    blockletInfo3List.add(blockletInfo3);
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex();
    byte[] startKey = footer.getBlockletIndex().getBtreeIndex().getStartKey();
    blockletBTreeIndex.setStart_key(startKey);
    byte[] endKey = footer.getBlockletIndex().getBtreeIndex().getEndKey();
    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    byte[][] maxValues = footer.getBlockletIndex().getMinMaxIndex().getMaxValues();
    for (int i = 0; i < maxValues.length; i++) {
      blockletMinMaxIndex.addToMax_values(ByteBuffer.wrap(maxValues[i]));
    }
    byte[][] minValues = footer.getBlockletIndex().getMinMaxIndex().getMinValues();
    for (int i = 0; i < minValues.length; i++) {
      blockletMinMaxIndex.addToMin_values(ByteBuffer.wrap(minValues[i]));
    }
    blockletBTreeIndex.setEnd_key(endKey);
    BlockletIndex blockletIndex = new BlockletIndex();
    blockletIndex.setMin_max_index(blockletMinMaxIndex);
    blockletIndex.setB_tree_index(blockletBTreeIndex);
    List<BlockletIndex> blockletIndexList = new ArrayList<>();
    blockletIndexList.add(blockletIndex);
    writeBlockletInfoToFile(blockletInfo3List, blockletIndexList);
    IndexHeader indexHeader = CarbonMetadataUtil
        .getIndexHeader(footer.getSegmentInfo().getColumnCardinality(), this.thriftColumnSchemaList,
            0, footer.getSchemaUpdatedTimeStamp());
    File carbondataFile = new File(filePath);
    BlockIndex index = new BlockIndex();
    index.offset = currentOffsetInFile;
    index.file_name = carbondataFile.getName();
    index.setNum_rows(footer.getNumberOfRows());
    index.setBlocklet_info(blockletInfo3);
    index.setBlock_index(blockletIndex);
    String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(carbondataFile.getName());
    long taskIdFromTaskNo = CarbonTablePath.DataFileUtil.getTaskIdFromTaskNo(taskNo);
    String bucketNo = CarbonTablePath.DataFileUtil.getBucketNo(carbondataFile.getName());
    int batchNoFromTaskNo = CarbonTablePath.DataFileUtil.getBatchNoFromTaskNo(taskNo);
    String timeStampFromFileName =
        CarbonTablePath.DataFileUtil.getTimeStampFromFileName(carbondataFile.getName());
    String path =
        carbondataFile.getParentFile().getAbsolutePath() + File.separator + CarbonTablePath
            .getCarbonIndexFileName(taskIdFromTaskNo, Integer.parseInt(bucketNo), batchNoFromTaskNo,
                "" + timeStampFromFileName);
    CarbonIndexFileWriter writer = new CarbonIndexFileWriter();
    // open file
    writer.openThriftWriter(path);
    // write the header first
    writer.writeThrift(indexHeader);
    // write the indexes
    writer.writeThrift(index);
    writer.close();
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

  public List<org.apache.carbondata.format.ColumnSchema> getColumnSchemaListAndCardinality(
      List<org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema> wrapperColumnSchemaList) {
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

  protected void writeBlockletInfoToFile(List<BlockletInfo3> blockletInfo3,
      List<BlockletIndex> blockletIndex) throws CarbonDataWriterException {
    try {
      // get the current file position
      long currentPosition = currentOffsetInFile;
      // get thrift file footer instance
      FileFooter3 convertFileMeta = CarbonMetadataUtil
          .convertFileFooterVersion3(blockletInfo3, blockletIndex,
              footer.getSegmentInfo().getColumnCardinality(), thriftColumnSchemaList.size());
      // fill the carbon index details
      byte[] byteArray = CarbonUtil.getByteArray(convertFileMeta);
      ByteBuffer buffer =
          ByteBuffer.allocate(byteArray.length + CarbonCommonConstants.LONG_SIZE_IN_BYTE);
      buffer.put(byteArray);
      buffer.putLong(currentPosition);
      buffer.flip();
      fileChannel.write(buffer);
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while writing the carbon file: ", e);
    }
  }

  /**
   * write file header
   */
  private void writeHeaderToFile() throws IOException {
    byte[] fileHeader = CarbonUtil.getByteArray(
        CarbonMetadataUtil.getFileHeader(true, thriftColumnSchemaList, this.schemaupdatedTime));
    ByteBuffer buffer = ByteBuffer.wrap(fileHeader);
    currentOffsetInFile += fileChannel.write(buffer);
  }
}

