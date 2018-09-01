/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.examples.sdk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.sdk.file.CarbonReader;
import org.apache.carbondata.sdk.file.CarbonSchemaReader;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;

/**
 * Example fo CarbonReader with close method
 * After readNextRow of CarbonReader, User should close the reader,
 * otherwise main will continue run some time
 */
public class CarbonReaderExample {
  public static void main(String[] args) {
    String path = args[0];
    int loop = Integer.parseInt(args[1]);
    String sort = "sort";
    if (args.length > 2) {
       sort = args[2];
    }
    int threads = 8;
    String ak = "LGCUKNYSCPHOLEH5UPZP";
    String sk = "1g7ttzgdelafd1v6teb1qR2iKqRzJpYQuOwl8rgw";
    String ep = "obs.cn-north-1.myhwclouds.com";
    if (args.length > 3) {
      ak = args[3];
      sk = args[4];
      ep = args[5];
    }
    if (args.length > 6) {
      threads = Integer.parseInt(args[6]);
    }

    System.out.println("ak: "+ ak);
    System.out.println("sk: "+ sk);
    System.out.println("ep: "+ ep);
    System.out.println("threads: "+ threads);

    //    String path = "./target/testWriteFiles";
    try {
      FileUtils.deleteDirectory(new File(path));

      Field[] fields = new Field[12];
      fields[0] = new Field("event_id", DataTypes.STRING);
      fields[1] = new Field("event_time", DataTypes.STRING);
      fields[2] = new Field("ingestion_time", DataTypes.STRING);
      fields[3] = new Field("subject", DataTypes.STRING);
      fields[4] = new Field("from_email", DataTypes.STRING);

      List<StructField> arr1 = new java.util.ArrayList<>();
      arr1.add(new StructField("to_email", DataTypes.STRING));
      fields[5] = new Field("to_email", "array", arr1);

      List<StructField> arr2 = new java.util.ArrayList<>();
      arr2.add(new StructField("cc_email", DataTypes.STRING));
      fields[6] = new Field("cc_email", "array", arr2);

      List<StructField> arr3 = new java.util.ArrayList<>();
      arr3.add(new StructField("bcc_email", DataTypes.STRING));
      fields[7] = new Field("bcc_email", "array", arr3);

      fields[8] = new Field("messagebody", DataTypes.STRING);

      List<StructField> arr4 = new java.util.ArrayList<>();
      arr4.add(new StructField("attachments", DataTypes.STRING));
      fields[9] = new Field("attachments", "array", arr4);

      fields[10] = new Field("link_to_original", DataTypes.STRING);
      fields[11] = new Field("protocol_info", DataTypes.STRING);

      Map<String, String> options = new HashMap<>();
      options.put("bad_records_action", "FORCE");
      options.put("complex_delimiter_level_1", "$");
      String csvPath = "/home/root1/Downloads/pro_table_last51-60.csv";
//      String csvPath = "/opt/p-project/pro_table_last51-60.csv";
      BufferedReader in =
          new BufferedReader(new FileReader(csvPath));

      String line = in.readLine();
      List<String[]> data = new ArrayList<>();
      try {
        while (line != null) {
          String[] alldata = line.split(",");
          if (alldata.length > 9) {
            if (alldata[8].length() > 31500) {
              alldata[8] = alldata[8].substring(0, 31499);
            }
            alldata[5] = alldata[5].replace(" ", "$");
            alldata[6] = alldata[6].replace(" ", "$");
            alldata[7] = alldata[7].replace(" ", "$");
            alldata[9] = alldata[9].replace(" ", "$");
          }
          data.add(alldata);
          line = in.readLine();
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        in.close();
        System.out.println(line);
      }
      ExecutorService service =  Executors.newFixedThreadPool(threads);
      FileFactory.getConfiguration().set("fs.s3a.access.key", ak);
      FileFactory.getConfiguration().set("fs.s3a.secret.key", sk);
      FileFactory.getConfiguration().set("fs.s3a.endpoint", ep);
      for (int k = 0; k < loop; k++) {
        CarbonWriterBuilder builder =
            CarbonWriter.builder().outputPath(path).withLoadOptions(options).setAccessKey(ak)
                .setSecretKey(sk).setEndPoint(ep);
        if (sort.equals("nosort")) {
          builder.sortBy(new String[0]);
        }
        CarbonWriter writer =
            builder.buildThreadSafeWriterForCSVInput(new Schema(fields), (short) threads);
        long start = System.currentTimeMillis();
        AtomicInteger counter = new AtomicInteger();
        int size  = 17*8;
        int dataSize = size/threads;
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
          futures.add(service.submit(new Runnable() {
            Random random = new Random();
            @Override public void run() {
              for (int i = 0; i < dataSize; i++) {

                for (String[] datum : data) {
                  try {
                    if (datum.length > 3) {
                      String[] newdatum = datum.clone();
                      newdatum[0] = newdatum[0] + random.nextLong();
                      newdatum[3] = newdatum[3] + random.nextLong();
                      writer.write(datum);
                    } else {
                      writer.write(datum);
                    }
                  } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }
                  counter.incrementAndGet();
                }
              }
            }
          }));
        }



        try {
          for (Future future : futures) {
            future.get();
          }
        } finally {
          writer.close();
          System.out.println(
              "%%%%%%%%%%%%%%%%% Time taken: " + (System.currentTimeMillis() - start) + " loop " + k
                  + " count " + counter.get());
        }
      }
      service.shutdown();

      //            File[] dataFiles = new File(path).listFiles(new FilenameFilter() {
      //                @Override
      //                public boolean accept(File dir, String name) {
      //                    if (name == null) {
      //                        return false;
      //                    }
      //                    return name.endsWith("carbonindex");
      //                }
      //            });
      //            if (dataFiles == null || dataFiles.length < 1) {
      //                throw new RuntimeException("Carbon index file not exists.");
      //            }
      //            Schema schema = CarbonSchemaReader
      //                .readSchemaInIndexFile(dataFiles[0].getAbsolutePath())
      //                .asOriginOrder();
      //            // Transform the schema
      //            String[] strings = new String[schema.getFields().length];
      //            for (int i = 0; i < schema.getFields().length; i++) {
      //                strings[i] = (schema.getFields())[i].getFieldName();
      //            }
      //
      //            // Read data
      //            CarbonReader reader = CarbonReader
      //                .builder(path, "_temp")
      //                .projection(strings)
      //                .build();
      //
      //            System.out.println("\nData:");
      //            long day = 24L * 3600 * 1000;
      //            int i = 0;
      //            while (reader.hasNext()) {
      //                Object[] row = (Object[]) reader.readNextRow();
      //                System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t",
      //                    i, row[0], row[1], row[2], row[3], row[4], row[5],
      //                    new Date((day * ((int) row[6]))), new Timestamp((long) row[7] / 1000), row[8]
      //                ));
      //                i++;
      //            }
      //            System.out.println("\nFinished");
      //
      //            // Read data
      //            CarbonReader reader2 = CarbonReader
      //                .builder(path, "_temp")
      //                .build();
      //
      //            System.out.println("\nData:");
      //            i = 0;
      //            while (reader2.hasNext()) {
      //              Object[] row = (Object[]) reader2.readNextRow();
      //              System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t",
      //                  i, row[0], new Date((day * ((int) row[1]))), new Timestamp((long) row[2] / 1000),
      //                  row[3], row[4], row[5], row[6], row[7], row[8]
      //              ));
      //              i++;
      //            }
      //            System.out.println("\nFinished");
      //            reader.close();
      //            FileUtils.deleteDirectory(new File(path));
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}
