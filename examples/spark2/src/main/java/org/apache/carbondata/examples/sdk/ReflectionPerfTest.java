package org.apache.carbondata.examples.sdk;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

public class ReflectionPerfTest {
  static final MethodHandle sfmh;
  static final MethodHandle sfmh1;

  static {
    try {
      Method m = Test.class.getMethod("putString", String.class);
      sfmh = MethodHandles.lookup().unreflect(m);
      Method m1 = Test.class.getMethod("putInt", int.class);
      sfmh1 = MethodHandles.lookup().unreflect(m1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws Throwable {
    final Test invocationTarget = new Test();
    final Method am = Test.class.getMethod("putString", String.class);
    am.setAccessible(true);
    final Method am1 = Test.class.getMethod("putInt", int.class);
    am1.setAccessible(true);

    long timeMillis = System.currentTimeMillis();

    for (int i = 0; i < 100000000; i++) {
      am.invoke(invocationTarget, "0");
      am1.invoke(invocationTarget, 0);
    }
    System.out.println("invoke : " +(System.currentTimeMillis() - timeMillis));

    timeMillis = System.currentTimeMillis();
    for (int i = 0; i < 100000000; i++) {
      sfmh.invokeExact(invocationTarget, "0");
      sfmh1.invokeExact(invocationTarget, 0);
    }
    System.out.println("invoke handle : " +(System.currentTimeMillis() - timeMillis));

    timeMillis = System.currentTimeMillis();
    for (int i = 0; i < 100000000; i++) {
      invocationTarget.putString("0");
      invocationTarget.putInt(0);
    }
    System.out.println("Direct : " +(System.currentTimeMillis() - timeMillis));
  }
}

class Test {

  private int[] a = new int[10];
  private String[] b = new String[10];

  public void putInt(int i) {
    a[0] = i;
  }

  public void putString(String i) {
    b[0] = i;
  }
}
