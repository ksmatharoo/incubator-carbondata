package org.apache.carbondata.examples.sdk;

import java.util.Arrays;
import java.util.Random;

public class RLEIntegeral {

  /**
   * types
   * 1.repeated / sequential
   * 2.Direct
   * Header
   * 1 bytes
   *   1 bit for type
   *   7 bits for size.
   * Remaining
   * Bit_pack
   * 1. packed in the multiples of values of n
   * 2. Bits are packed in multiples of 2, 4, 8, 12,14, 16,18, 20,22, 24, 28, 32, 38, 40.
   */

  public static void main(String[] args) {
    //    test12packBits();
    //    test2packBits();
    //    test4packBits();
//    test6packBits();
//    test8packBits();
//    test10packBits();
//    test14packBits();
//    test16packBits();
//    test18packBits();
//    test20packBits();
//    test22packBits();
//    test24packBits();
//    test28packBits();
    test32packBits();
  }

  private static void test2packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(3);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 2;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf2(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf2(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test4packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(15);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 4;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf4(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf4(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test6packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(64);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 6;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf6(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf6(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test8packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(256);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 8;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf8(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf8(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test10packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(1024);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 10;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf10(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf10(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test12packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(4095);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 12;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf12(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf12(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test14packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(16384);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 14;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf14(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf14(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test16packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(65536);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 16;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf16(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf16(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test18packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(262144);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 18;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf18(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf18(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test20packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(1048576);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 20;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf20(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf20(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test22packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(4194304);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 22;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf22(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf22(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test24packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(16777216);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 24;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf24(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf24(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test28packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(268435456);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 28;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf28(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf28(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void test32packBits() {
    long[] data = new long[100];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(Integer.MAX_VALUE);
    }
    System.out.println(Arrays.toString(data));
    int toallen = data.length * 32;
    int len = toallen / 8;
    if (toallen % 8 > 0) {
      len += 1;
    }
    byte[] encoded = new byte[len];
    packBitsOf32(data, encoded, data.length);
    long[] decoded = new long[data.length];
    unpackBitsOf32(encoded, decoded, data.length);
    System.out.println(Arrays.toString(decoded));
  }

  private static void packBitsOf2(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k++) {
      out[k] = (byte) (
          (((in[i] & 3)) | ((in[i + 1] & 3) << 2) | ((in[i + 2] & 3) << 4) | ((in[i + 3] & 3) << 6))
              & 255);
    }
  }

  private static void unpackBitsOf2(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k++) {
      out[i] = (((((int) in[k]) & 255)) & 3);
      out[i + 1] = (((((int) in[k]) & 255) >>> 2) & 3);
      out[i + 2] = (((((int) in[k]) & 255) >>> 4) & 3);
      out[i + 3] = (((((int) in[k]) & 255) >>> 6) & 3);

    }
  }

  private static void packBitsOf4(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 2, k++) {
      out[k] = (byte) ((((in[i] & 15)) | ((in[i + 1] & 15) << 4)) & 255);
    }
  }

  private static void unpackBitsOf4(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 2, k++) {
      out[i] = (((((int) in[k]) & 255)) & 15);
      out[i + 1] = (((((int) in[k]) & 255) >>> 4) & 15);

    }
  }

  private static void packBitsOf6(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k += 3) {
      out[k] = (byte) ((((in[i] & 63)) | ((in[i + 1] & 63) << 6)) & 255);
      out[k + 1] = (byte) ((((in[i + 1] & 63) >>> 2) | ((in[i + 2] & 63) << 4)) & 255);
      out[k + 2] = (byte) ((((in[i + 2] & 63) >>> 4) | ((in[i + 3] & 63) << 2)) & 255);
    }
  }

  private static void unpackBitsOf6(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k += 3) {
      out[i] = (((((int) in[k]) & 255)) & 63);
      out[i + 1] = (((((int) in[k]) & 255) >>> 6) & 63) | (((((int) in[k + 1]) & 255) << 2) & 63);
      out[i + 2] =
          (((((int) in[k + 1]) & 255) >>> 4) & 63) | (((((int) in[k + 2]) & 255) << 4) & 63);
      out[i + 3] = (((((int) in[k + 2]) & 255) >>> 2) & 63);
    }
  }

  private static void packBitsOf8(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i++, k++) {
      out[k] = (byte) (in[i]);
    }
  }

  private static void unpackBitsOf8(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i++, k++) {
      out[i] = in[k];
    }
  }

  private static void packBitsOf10(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k += 5) {
      out[k] = (byte) ((((in[i] & 1023))) & 255);
      out[k + 1] = (byte) ((((in[i] & 1023) >>> 8) | ((in[i + 1] & 1023) << 2)) & 255);
      out[k + 2] = (byte) ((((in[i + 1] & 1023) >>> 6) | ((in[i + 2] & 1023) << 4)) & 255);
      out[k + 3] = (byte) ((((in[i + 2] & 1023) >>> 4) | ((in[i + 3] & 1023) << 6)) & 255);
      out[k + 4] = (byte) ((((in[i + 3] & 1023) >>> 2)) & 255);
    }
  }

  private static void unpackBitsOf10(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k += 5) {
      out[i] = (((((int) in[k]) & 255)) & 1023) | (((((int) in[k + 1]) & 255) << 8) & 1023);
      out[i + 1] =
          (((((int) in[k + 1]) & 255) >>> 2) & 1023) | (((((int) in[k + 2]) & 255) << 6) & 1023);
      out[i + 2] =
          (((((int) in[k + 2]) & 255) >>> 4) & 1023) | (((((int) in[k + 3]) & 255) << 4) & 1023);
      out[i + 3] =
          (((((int) in[k + 3]) & 255) >>> 6) & 1023) | (((((int) in[k + 4]) & 255) << 2) & 1023);
    }
  }

  private static void packBitsOf12(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 2, k += 3) {
      long first = in[i] & 4095;
      out[k] = (byte) (first & 255);
      out[k + 1] = (byte) (((first >>> 8) | ((in[i + 1] & 4095) << 4)) & 255);
      out[k + 2] = (byte) ((((in[i + 1] & 4095) >>> 4)) & 255);
    }
  }

  private static void unpackBitsOf12(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 2, k += 3) {
      out[i] = (((((long) in[k]) & 255)) & 4095) | (((((long) in[k + 1]) & 255) << 8) & 4095);
      out[i + 1] =
          (((((long) in[k + 1]) & 255) >>> 4) & 4095) | (((((long) in[k + 2]) & 255) << 4) & 4095);
    }
  }

  private static void packBitsOf14(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k += 7) {
      out[k] = (byte) ((((in[i] & 16383))) & 255);
      out[k + 1] = (byte) ((((in[i] & 16383) >>> 8) | ((in[i + 1] & 16383) << 6)) & 255);
      out[k + 2] = (byte) ((((in[i + 1] & 16383) >>> 2)) & 255);
      out[k + 3] = (byte) ((((in[i + 1] & 16383) >>> 10) | ((in[i + 2] & 16383) << 4)) & 255);
      out[k + 4] = (byte) ((((in[i + 2] & 16383) >>> 4)) & 255);
      out[k + 5] = (byte) ((((in[i + 2] & 16383) >>> 12) | ((in[i + 3] & 16383) << 2)) & 255);
      out[k + 6] = (byte) ((((in[i + 3] & 16383) >>> 6)) & 255);
    }
  }

  private static void unpackBitsOf14(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k += 7) {
      out[i] = (((((int) in[k]) & 255)) & 16383) | (((((int) in[k + 1]) & 255) << 8) & 16383);
      out[i + 1] =
          (((((int) in[k + 1]) & 255) >>> 6) & 16383) | (((((int) in[k + 2]) & 255) << 2) & 16383)
              | (((((int) in[k + 3]) & 255) << 10) & 16383);
      out[i + 2] =
          (((((int) in[k + 3]) & 255) >>> 4) & 16383) | (((((int) in[k + 4]) & 255) << 4) & 16383)
              | (((((int) in[k + 5]) & 255) << 12) & 16383);
      out[i + 3] =
          (((((int) in[k + 5]) & 255) >>> 2) & 16383) | (((((int) in[k + 6]) & 255) << 6) & 16383);
    }
  }

  private static void packBitsOf16(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i++, k += 2) {
      out[k] = (byte) ((((in[i] & 65535))) & 255);
      out[k + 1] = (byte) ((((in[i] & 65535) >>> 8)) & 255);
    }
  }

  private static void unpackBitsOf16(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i ++, k += 2) {
      out[i] = (((((int)in[k]) & 255) ) & 65535) | (((((int)in[k+1]) & 255) <<  8) & 65535);
    }
  }

  private static void packBitsOf18(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k += 9) {
      out[k] = (byte) ((((in[i] & 262143))) & 255);
      out[k + 1] = (byte) ((((in[i] & 262143) >>> 8)) & 255);
      out[k + 2] = (byte) ((((in[i] & 262143) >>> 16) | ((in[i + 1] & 262143) << 2)) & 255);
      out[k + 3] = (byte) ((((in[i + 1] & 262143) >>> 6)) & 255);
      out[k + 4] = (byte) ((((in[i + 1] & 262143) >>> 14) | ((in[i + 2] & 262143) << 4)) & 255);
      out[k + 5] = (byte) ((((in[i + 2] & 262143) >>> 4)) & 255);
      out[k + 6] = (byte) ((((in[i + 2] & 262143) >>> 12) | ((in[i + 3] & 262143) << 6)) & 255);
      out[k + 7] = (byte) ((((in[i + 3] & 262143) >>> 2)) & 255);
      out[k + 8] = (byte) ((((in[i + 3] & 262143) >>> 10)) & 255);
    }
  }

  private static void unpackBitsOf18(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k += 9) {
      out[i] = (((((int) in[k]) & 255)) & 262143) | (((((int) in[k + 1]) & 255) << 8) & 262143) | (
          ((((int) in[k + 2]) & 255) << 16) & 262143);
      out[i + 1] =
          (((((int) in[k + 2]) & 255) >>> 2) & 262143) | (((((int) in[k + 3]) & 255) << 6) & 262143)
              | (((((int) in[k + 4]) & 255) << 14) & 262143);
      out[i + 2] =
          (((((int) in[k + 4]) & 255) >>> 4) & 262143) | (((((int) in[k + 5]) & 255) << 4) & 262143)
              | (((((int) in[k + 6]) & 255) << 12) & 262143);
      out[i + 3] =
          (((((int) in[k + 6]) & 255) >>> 6) & 262143) | (((((int) in[k + 7]) & 255) << 2) & 262143)
              | (((((int) in[k + 8]) & 255) << 10) & 262143);
    }
  }

  private static void packBitsOf20(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 2, k += 5) {
      out[k] = (byte) ((((in[i] & 1048575))) & 255);
      out[k + 1] = (byte) ((((in[i] & 1048575) >>> 8)) & 255);
      out[k + 2] = (byte) ((((in[i] & 1048575) >>> 16) | ((in[i + 1] & 1048575) << 4)) & 255);
      out[k + 3] = (byte) ((((in[i + 1] & 1048575) >>> 4)) & 255);
      out[k + 4] = (byte) ((((in[i + 1] & 1048575) >>> 12)) & 255);
    }
  }

  private static void unpackBitsOf20(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 2, k += 5) {
      out[i] =
          (((((int) in[k]) & 255)) & 1048575) | (((((int) in[k + 1]) & 255) << 8) & 1048575) | (
              ((((int) in[k + 2]) & 255) << 16) & 1048575);
      out[i + 1] = (((((int) in[k + 2]) & 255) >>> 4) & 1048575) | (((((int) in[k + 3]) & 255) << 4)
          & 1048575) | (((((int) in[k + 4]) & 255) << 12) & 1048575);
    }
  }

  private static void packBitsOf22(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k += 11) {
      out[k] = (byte) ((((in[i] & 4194303))) & 255);
      out[k + 1] = (byte) ((((in[i] & 4194303) >>> 8)) & 255);
      out[k + 2] = (byte) ((((in[i] & 4194303) >>> 16) | ((in[i + 1] & 4194303) << 6)) & 255);
      out[k + 3] = (byte) ((((in[i + 1] & 4194303) >>> 2)) & 255);
      out[k + 4] = (byte) ((((in[i + 1] & 4194303) >>> 10)) & 255);
      out[k + 5] = (byte) ((((in[i + 1] & 4194303) >>> 18) | ((in[i + 2] & 4194303) << 4)) & 255);
      out[k + 6] = (byte) ((((in[i + 2] & 4194303) >>> 4)) & 255);
      out[k + 7] = (byte) ((((in[i + 2] & 4194303) >>> 12)) & 255);
      out[k + 8] = (byte) ((((in[i + 2] & 4194303) >>> 20) | ((in[i + 3] & 4194303) << 2)) & 255);
      out[k + 9] = (byte) ((((in[i + 3] & 4194303) >>> 6)) & 255);
      out[k + 10] = (byte) ((((in[i + 3] & 4194303) >>> 14)) & 255);
    }
  }

  private static void unpackBitsOf22(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 4, k += 11) {
      out[i] =
          (((((int) in[k]) & 255)) & 4194303) | (((((int) in[k + 1]) & 255) << 8) & 4194303) | (
              ((((int) in[k + 2]) & 255) << 16) & 4194303);
      out[i + 1] = (((((int) in[k + 2]) & 255) >>> 6) & 4194303) | (((((int) in[k + 3]) & 255) << 2)
          & 4194303) | (((((int) in[k + 4]) & 255) << 10) & 4194303) | (
          ((((int) in[k + 5]) & 255) << 18) & 4194303);
      out[i + 2] = (((((int) in[k + 5]) & 255) >>> 4) & 4194303) | (((((int) in[k + 6]) & 255) << 4)
          & 4194303) | (((((int) in[k + 7]) & 255) << 12) & 4194303) | (
          ((((int) in[k + 8]) & 255) << 20) & 4194303);
      out[i + 3] = (((((int) in[k + 8]) & 255) >>> 2) & 4194303) | (((((int) in[k + 9]) & 255) << 6)
          & 4194303) | (((((int) in[k + 10]) & 255) << 14) & 4194303);
    }
  }

  private static void packBitsOf24(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i++, k += 3) {
      out[k] = (byte) ((((in[i] & 16777215))) & 255);
      out[k + 1] = (byte) ((((in[i] & 16777215) >>> 8)) & 255);
      out[k + 2] = (byte) ((((in[i] & 16777215) >>> 16)) & 255);
    }
  }

  private static void unpackBitsOf24(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i++, k += 3) {
      out[i] =
          (((((int) in[k]) & 255)) & 16777215) | (((((int) in[k + 1]) & 255) << 8) & 16777215) | (
              ((((int) in[k + 2]) & 255) << 16) & 16777215);
    }
  }

  private static void packBitsOf28(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 2, k += 7) {
      out[k] = (byte) ((((in[i] & 268435455))) & 255);
      out[k + 1] = (byte) ((((in[i] & 268435455) >>> 8)) & 255);
      out[k + 2] = (byte) ((((in[i] & 268435455) >>> 16)) & 255);
      out[k + 3] = (byte) ((((in[i] & 268435455) >>> 24) | ((in[i + 1] & 268435455) << 4)) & 255);
      out[k + 4] = (byte) ((((in[i + 1] & 268435455) >>> 4)) & 255);
      out[k + 5] = (byte) ((((in[i + 1] & 268435455) >>> 12)) & 255);
      out[k + 6] = (byte) ((((in[i + 1] & 268435455) >>> 20)) & 255);
    }
  }

  private static void unpackBitsOf28(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i += 2, k += 7) {
      out[i] =
          (((((int) in[k]) & 255)) & 268435455) | (((((int) in[k + 1]) & 255) << 8) & 268435455) | (
              ((((int) in[k + 2]) & 255) << 16) & 268435455) | (((((int) in[k + 3]) & 255) << 24)
              & 268435455);
      out[i + 1] =
          (((((int) in[k + 3]) & 255) >>> 4) & 268435455) | (((((int) in[k + 4]) & 255) << 4)
              & 268435455) | (((((int) in[k + 5]) & 255) << 12) & 268435455) | (
              ((((int) in[k + 6]) & 255) << 20) & 268435455);
    }
  }

  private static void packBitsOf32(long[] in, byte[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i++, k += 4) {
      out[k] = (byte) ((((in[i] & -1))) & 255);
      out[k + 1] = (byte) ((((in[i] & -1) >>> 8)) & 255);
      out[k + 2] = (byte) ((((in[i] & -1) >>> 16)) & 255);
      out[k + 3] = (byte) ((((in[i] & -1) >>> 24)) & 255);
    }
  }

  private static void unpackBitsOf32(byte[] in, long[] out, int len) {
    int k = 0;
    for (int i = 0; i < len; i++, k += 4) {
      out[i] = (((((int) in[k]) & 255)) & -1) | (((((int) in[k + 1]) & 255) << 8) & -1) | (
          ((((int) in[k + 2]) & 255) << 16) & -1) | (((((int) in[k + 3]) & 255) << 24) & -1);
    }
  }
}
