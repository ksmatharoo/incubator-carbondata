package org.apache.carbondata.examples.sdk;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EmailParser {

  public static void main(String[] args) throws IOException, MessagingException {

    String input = args[0];
    String output = args[1];

    String[] headers = new String[12];
    headers[0] = "Message-ID";
    headers[1] = "Date";
    headers[2] = "Date";
    headers[3] = "Subject";
    headers[4] = "X-From";
    headers[5] = "X-To";
    headers[6] = "X-cc";
    headers[7] = "X-bcc";
    headers[8] = "body";
    headers[9] = "X-FileName";
    headers[10] = "X-Origin";
    headers[11] = "Content-Type";

    File file = new File(input);

    File[] files = file.listFiles();
    new File(output).mkdirs();
    int count = 0;
    for (File file1 : files) {
      List<File> list = new ArrayList<>();
      extractFiles(file1, list);

      BufferedWriter writer = new BufferedWriter(new FileWriter(output + "/"+file1.getName()+".csv"));
      for (File file2 : list) {
        String[] line = parse(headers, file2);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < line.length; i++) {
          if (i < line.length - 1 ) {
            builder.append(line[i]).append(",");
          } else {
            builder.append(line[i]);
          }
        }
        writer.write(builder.toString());
        writer.newLine();
      }
      System.out.println("Number of files written in file "+file1.getName() + " count: "+ list.size());
      count += list.size();
      writer.close();
    }
    System.out.println("Total records: "+ count);
  }

  private static void extractFiles(File file, List<File> files) {
    if (file.isDirectory()) {
      File[] files1 = file.listFiles();
      for (File file1 : files1) {
        extractFiles(file1, files);
      }
    } else {
      files.add(file);
    }
  }

  private static String[] parse(String[] headers, File path) throws MessagingException, IOException {

    FileInputStream stream = new FileInputStream(path);

    MimeMessage parser = new MimeMessage(null, stream);
    String[] line = new String[headers.length];

    for (int i = 0; i < line.length; i++) {
      if (i == 8) {
        line[i] = parser.getContent().toString().replaceAll("[\t\n\r]", "").replace(",", "");
        if (line[i].length() > 31900) {
          line[i] = line[i].substring(0, 31899);
        }
      } else {
        String header = parser.getHeader(headers[i], "$");
        if (header == null) {
          line[i] = "";
        } else {
          if (i == 5 || i == 6 || i == 7 || i == 9) {
            if (header.length() > 30000) {
              header = header.substring(0, 29999);
            }
            line[i] = header.replaceAll("[\t\n\r]", "").replace(",", "$");
          } else {
            line[i] = header.replaceAll("[\t\n\r]", "").replace(",", "");
          }
        }
      }
    }
    stream.close();
    return line;
  }
}
