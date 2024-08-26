package cn.edu.tsinghua.iot.benchmark.utils;

import java.util.Formatter;

public class BlobUtils {
  private BlobUtils() {}

  public static String bytesToHex(byte[] bytes) {
    String hex = "";
    try (Formatter formatter = new Formatter()) {
      for (byte b : bytes) {
        formatter.format("%02X", b);
      }
      hex = formatter.toString();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return hex;
  }

  public static String stringToHex(String input) {
    byte[] bytes = input.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    return bytesToHex(bytes);
  }
}
