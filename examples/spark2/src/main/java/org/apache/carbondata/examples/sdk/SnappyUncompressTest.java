package org.apache.carbondata.examples.sdk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.compression.SnappyCompressor;
import org.apache.carbondata.core.util.ByteUtil;

import org.xerial.snappy.Snappy;

public class SnappyUncompressTest {

  public static void main(String[] args) throws IOException {

    int[] a = new int[32000];

    Random random = new Random();
    for (int i = 0; i < a.length; i++) {
      a[i] = random.nextInt(15000);
    }

    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    byte[] bytes = compressor.compressInt(a);
    byte[] uncompressedBytes = new byte[32000*4];
    long j = System.currentTimeMillis();
    for (int l = 0; l < 1000; l++) {
//            float[] ints = compressor.unCompressInt(bytes, 0, bytes.length);
      int uncompressedLength = Snappy.uncompressedLength(bytes, 0, bytes.length);
      compressor.rawUncompress(bytes, uncompressedBytes);
      ByteBuffer buffer = ByteBuffer.wrap(uncompressedBytes).order(ByteOrder.LITTLE_ENDIAN);
      int k =0;
      for (int i = 0; i < a.length; i++, k+=4) {
        int toInt = toInt(uncompressedBytes, k);
//        int toInt = toIntBuffer(buffer);
//        if (a[i] != toInt) {
//          System.out.println("Not : "+ a[i] +" != "+toInt);
//        }
      }
    }
    System.out.println("Time: "+ (System.currentTimeMillis()-j));

  }

  public static int toInt(byte[] bytes, int offset) {
    return (((int)bytes[offset+3] & 0xff) << 24) + (((int)bytes[offset + 2] & 0xff) << 16) +
        (((int)bytes[offset + 1] & 0xff) << 8) + ((int)bytes[offset] & 0xff);
  }

  public static int toIntBuffer(ByteBuffer byteBuffer) {
    return byteBuffer.getInt();
  }

  public static short toShort(byte[] bytes, int offset) {
    return (short) ((((int)bytes[offset + 1] & 0xff) << 8) + ((int)bytes[offset] & 0xff));
  }

  public static double toDouble(byte[] bytes, int offset) {
    return Double.longBitsToDouble(toLongNew(bytes, offset));
  }

  public static long toLongNew(byte[] bytes, int offset) {
    return ((((long)bytes[offset+7]       ) << 56) |
        (((long)bytes[offset + 6] & 0xff) << 48) |
        (((long)bytes[offset + 5] & 0xff) << 40) |
        (((long)bytes[offset + 4] & 0xff) << 32) |
        (((long)bytes[offset + 3] & 0xff) << 24) |
        (((long)bytes[offset + 2] & 0xff) << 16) |
        (((long)bytes[offset + 1] & 0xff) <<  8) |
        (((long)bytes[offset] & 0xff)      ));
  }
}
