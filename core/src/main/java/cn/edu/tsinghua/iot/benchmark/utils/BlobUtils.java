package cn.edu.tsinghua.iot.benchmark.utils;

import java.util.Formatter;

public class BlobUtils {
  private BlobUtils() {}

  public static String bytesToHex(byte[] bytes) {
    Formatter formatter = new Formatter();
    for (byte b : bytes) {
      formatter.format("%02X", b);
    }
    String hex = formatter.toString();
    formatter.close();
    return hex;
  }

  public static String stringToHex(String input) {
    byte[] bytes = input.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    return bytesToHex(bytes);
  }
}
