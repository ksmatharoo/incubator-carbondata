package org.apache.carbondata.processing.newflow.unsafe;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

public class UnsafeData {
  private static Unsafe unsafe;

  /**
   * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
   * allow safepoint polling during a large copy.
   */
  private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;
  static {
    try {
      Field cause = Unsafe.class.getDeclaredField("theUnsafe");
      cause.setAccessible(true);
      unsafe = (Unsafe)cause.get((Object)null);
    } catch (Throwable var2) {
      unsafe = null;
    }
  }
  public static long allocateMemory(long size) {
    return unsafe.allocateMemory(size);
  }

  public static void freeMemory(long address) {
    unsafe.freeMemory(address);
  }

  public static void copyMemory(
      Object src, long srcOffset, Object dst, long dstOffset, long length) {
    // Check if dstOffset is before or after srcOffset to determine if we should copy
    // forward or backwards. This is necessary in case src and dst overlap.
    if (dstOffset < srcOffset) {
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        unsafe.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
        srcOffset += size;
        dstOffset += size;
      }
    } else {
      srcOffset += length;
      dstOffset += length;
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        srcOffset -= size;
        dstOffset -= size;
        unsafe.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
      }

    }
  }

}
