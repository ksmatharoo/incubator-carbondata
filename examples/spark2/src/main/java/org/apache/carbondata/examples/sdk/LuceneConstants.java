package org.apache.carbondata.examples.sdk;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.processing.loading.converter.impl.binary.Base64BinaryDecoder;

import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

public class LuceneConstants {
  public static final String CONTENTS = "contents";
  public static final String FILE_NAME = "filename";
  public static final String FILE_PATH = "filepath";
  public static final int MAX_SEARCH = 10;

  public static void main(String[] args) throws IOException {
//    PythonInterpreter python = new PythonInterpreter();
//    int number1 = 10;
//    int number2 = 32;
//    python.exec("import numpy as np\n" + "\n" + "# Make the array `my_array`\n"
//        + "my_array = np.array([[1,2,3,4], [5,6,7,8]], dtype=np.int64)\n" + "\n"
//        + "# Print `my_array`\n" + "print(my_array)");
//    python.set("number1", new PyInteger(number1));
//    python.set("number2", new PyInteger(number2));
//    python.exec("number3 = number1 + number2");
//    PyObject number3 = python.get("number3");
//    System.out.println("Val : " + number3.toString());

    String path = "/home/root1/corpput.txt";
    new Thread() {
      @Override public void run() {
        try {
          DataOutputStream dataOutputStream = FileFactory
              .getDataOutputStream(path, FileFactory.FileType.LOCAL,
                  CarbonCommonConstants.BYTEBUFFER_SIZE, true);
          write(dataOutputStream, "uvwxyz");
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }.start();

    new Thread() {
      @Override public void run() {
        try {
          DataOutputStream dataOutputStream = FileFactory
              .getDataOutputStream(path, FileFactory.FileType.LOCAL,
                  CarbonCommonConstants.BYTEBUFFER_SIZE, true);
          write(dataOutputStream, "abcdef");
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }.start();
  }

  private static void write(DataOutputStream dataOutputStream, String string) throws IOException {
    int i = 0;
    while(i < 100000) {
      dataOutputStream.writeUTF(string);
      i++;
      if (i % 100 == 0) {
        dataOutputStream.writeUTF("\n");
      }
    }
    dataOutputStream.close();
  }
}
