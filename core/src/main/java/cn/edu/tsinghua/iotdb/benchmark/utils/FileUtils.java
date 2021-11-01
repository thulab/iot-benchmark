package cn.edu.tsinghua.iotdb.benchmark.utils;

import java.io.File;

public class FileUtils {

  /** Union path by seperator */
  public static String union(String... path) {
    return String.join(File.separator, path);
  }
}
