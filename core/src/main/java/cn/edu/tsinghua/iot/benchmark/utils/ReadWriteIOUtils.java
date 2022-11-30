/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * ConverterUtils is a utility class. It provide conversion between normal datatype and byte array.
 */
public class ReadWriteIOUtils {

  private static final int SHORT_LEN = 2;
  private static final int INT_LEN = 4;
  private static final int LONG_LEN = 8;
  private static final int DOUBLE_LEN = 8;
  private static final int FLOAT_LEN = 4;

  private ReadWriteIOUtils() {}

  /** read a bool from inputStream. */
  public static boolean readBool(InputStream inputStream) throws IOException {
    int flag = inputStream.read();
    return flag == 1;
  }

  /** read a bool from byteBuffer. */
  public static boolean readBool(ByteBuffer buffer) {
    byte a = buffer.get();
    return a == 1;
  }

  /** read a byte from byteBuffer. */
  public static byte readByte(ByteBuffer buffer) {
    return buffer.get();
  }

  /**
   * read bytes array in given size
   *
   * @param buffer buffer
   * @param size size
   * @return bytes array
   */
  public static byte[] readBytes(ByteBuffer buffer, int size) {
    byte[] res = new byte[size];
    buffer.get(res);
    return res;
  }

  /** write if the object equals null. Eg, object equals null, then write true. */
  public static int writeIsNull(Object object, OutputStream outputStream) throws IOException {
    return write(object == null, outputStream);
  }

  /** write if the object equals null. Eg, object equals null, then write true. */
  public static int writeIsNull(Object object, ByteBuffer buffer) {
    return write(object == null, buffer);
  }

  /** read a bool from byteBuffer. */
  public static boolean readIsNull(InputStream inputStream) throws IOException {
    return readBool(inputStream);
  }

  /** read a bool from byteBuffer. */
  public static boolean readIsNull(ByteBuffer buffer) {
    return readBool(buffer);
  }

  public static int write(Map<String, String> map, DataOutputStream stream) throws IOException {
    int length = 0;
    byte[] bytes;
    stream.writeInt(map.size());
    length += 4;
    for (Entry<String, String> entry : map.entrySet()) {
      bytes = entry.getKey().getBytes();
      stream.writeInt(bytes.length);
      length += 4;
      stream.write(bytes);
      length += bytes.length;
      bytes = entry.getValue().getBytes();
      stream.writeInt(bytes.length);
      length += 4;
      stream.write(bytes);
      length += bytes.length;
    }
    return length;
  }

  /**
   * write a int value to outputStream according to flag. If flag is true, write 1, else write 0.
   */
  public static int write(Boolean flag, OutputStream outputStream) throws IOException {
    if (flag) {
      outputStream.write(1);
    } else {
      outputStream.write(0);
    }
    return 1;
  }

  /** write a byte to byteBuffer according to flag. If flag is true, write 1, else write 0. */
  public static int write(Boolean flag, ByteBuffer buffer) {
    byte a;
    if (flag) {
      a = 1;
    } else {
      a = 0;
    }

    buffer.put(a);
    return 1;
  }

  /**
   * write a byte n.
   *
   * @return The number of bytes used to represent a {@code byte} value in two's complement binary
   *     form.
   */
  public static int write(byte n, OutputStream outputStream) throws IOException {
    outputStream.write(n);
    return Byte.BYTES;
  }

  /**
   * write a short n.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(short n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.shortToBytes(n);
    outputStream.write(bytes);
    return bytes.length;
  }

  /**
   * write a byte n to byteBuffer.
   *
   * @return The number of bytes used to represent a {@code byte} value in two's complement binary
   *     form.
   */
  public static int write(byte n, ByteBuffer buffer) {
    buffer.put(n);
    return Byte.BYTES;
  }

  /**
   * write a short n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(short n, ByteBuffer buffer) {
    buffer.putShort(n);
    return SHORT_LEN;
  }

  /**
   * write a int n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(int n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.intToBytes(n);
    outputStream.write(bytes);
    return INT_LEN;
  }

  /**
   * write a int n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(int n, ByteBuffer buffer) {
    buffer.putInt(n);
    return INT_LEN;
  }

  /**
   * write a float n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(float n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.floatToBytes(n);
    outputStream.write(bytes);
    return FLOAT_LEN;
  }

  /**
   * write a double n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(double n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.doubleToBytes(n);
    outputStream.write(bytes);
    return DOUBLE_LEN;
  }

  /**
   * write a long n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(long n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.longToBytes(n);
    outputStream.write(bytes);
    return LONG_LEN;
  }

  /** write a long n to byteBuffer. */
  public static int write(long n, ByteBuffer buffer) {
    buffer.putLong(n);
    return LONG_LEN;
  }

  /** write a float n to byteBuffer. */
  public static int write(float n, ByteBuffer buffer) {
    buffer.putFloat(n);
    return FLOAT_LEN;
  }

  /** write a double n to byteBuffer. */
  public static int write(double n, ByteBuffer buffer) {
    buffer.putDouble(n);
    return DOUBLE_LEN;
  }

  /**
   * write string to outputStream.
   *
   * @return the length of string represented by byte[].
   */
  public static int write(String s, OutputStream outputStream) throws IOException {
    int len = 0;
    byte[] bytes = s.getBytes();
    len += write(bytes.length, outputStream);
    outputStream.write(bytes);
    len += bytes.length;
    return len;
  }

  /**
   * write string to byteBuffer.
   *
   * @return the length of string represented by byte[].
   */
  public static int write(String s, ByteBuffer buffer) {
    if (s == null) {
      return write(-1, buffer);
    }
    int len = 0;
    byte[] bytes = s.getBytes();
    len += write(bytes.length, buffer);
    buffer.put(bytes);
    len += bytes.length;
    return len;
  }

  /** write byteBuffer.capacity and byteBuffer.array to outputStream. */
  public static int write(ByteBuffer byteBuffer, OutputStream outputStream) throws IOException {
    int len = 0;
    len += write(byteBuffer.capacity(), outputStream);
    byte[] bytes = byteBuffer.array();
    outputStream.write(bytes);
    len += bytes.length;
    return len;
  }

  /** write byteBuffer.array to outputStream without capacity. */
  public static int writeWithoutSize(ByteBuffer byteBuffer, OutputStream outputStream)
      throws IOException {
    byte[] bytes = byteBuffer.array();
    outputStream.write(bytes);
    return bytes.length;
  }

  /** write byteBuffer.capacity and byteBuffer.array to byteBuffer. */
  public static int write(ByteBuffer byteBuffer, ByteBuffer buffer) {
    int len = 0;
    len += write(byteBuffer.capacity(), buffer);
    byte[] bytes = byteBuffer.array();
    buffer.put(bytes);
    len += bytes.length;
    return len;
  }

  /** read a short var from inputStream. */
  public static short readShort(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[SHORT_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != SHORT_LEN) {
      throw new IOException(
          String.format(
              "Intend to read %d bytes but %d are actually returned", SHORT_LEN, readLen));
    }
    return BytesUtils.bytesToShort(bytes);
  }

  /** read a short var from byteBuffer. */
  public static short readShort(ByteBuffer buffer) {
    return buffer.getShort();
  }

  /** read a float var from inputStream. */
  public static float readFloat(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[FLOAT_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != FLOAT_LEN) {
      throw new IOException(
          String.format(
              "Intend to read %d bytes but %d are actually returned", FLOAT_LEN, readLen));
    }
    return BytesUtils.bytesToFloat(bytes);
  }

  /** read a float var from byteBuffer. */
  public static float readFloat(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[FLOAT_LEN];
    byteBuffer.get(bytes);
    return BytesUtils.bytesToFloat(bytes);
  }

  /** read a double var from inputStream. */
  public static double readDouble(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[DOUBLE_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != DOUBLE_LEN) {
      throw new IOException(
          String.format(
              "Intend to read %d bytes but %d are actually returned", DOUBLE_LEN, readLen));
    }
    return BytesUtils.bytesToDouble(bytes);
  }

  /** read a double var from byteBuffer. */
  public static double readDouble(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[DOUBLE_LEN];
    byteBuffer.get(bytes);
    return BytesUtils.bytesToDouble(bytes);
  }

  /** read a int var from inputStream. */
  public static int readInt(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[INT_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != INT_LEN) {
      throw new IOException(
          String.format("Intend to read %d bytes but %d are actually returned", INT_LEN, readLen));
    }
    return BytesUtils.bytesToInt(bytes);
  }

  /** read a int var from byteBuffer. */
  public static int readInt(ByteBuffer buffer) {
    return buffer.getInt();
  }

  /**
   * read an unsigned byte(0 ~ 255) as InputStream does.
   *
   * @return the byte or -1(means there is no byte to read)
   */
  public static int read(ByteBuffer buffer) {
    if (!buffer.hasRemaining()) {
      return -1;
    }
    return buffer.get() & 0xFF;
  }

  /** read a long var from inputStream. */
  public static long readLong(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[LONG_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != LONG_LEN) {
      throw new IOException(
          String.format("Intend to read %d bytes but %d are actually returned", LONG_LEN, readLen));
    }
    return BytesUtils.bytesToLong(bytes);
  }

  /** read a long var from byteBuffer. */
  public static long readLong(ByteBuffer buffer) {
    return buffer.getLong();
  }

  /** read string from inputStream. */
  public static String readString(InputStream inputStream) throws IOException {
    int strLength = readInt(inputStream);
    byte[] bytes = new byte[strLength];
    int readLen = inputStream.read(bytes, 0, strLength);
    if (readLen != strLength) {
      throw new IOException(
          String.format(
              "Intend to read %d bytes but %d are actually returned", strLength, readLen));
    }
    return new String(bytes, 0, strLength);
  }

  /** read string from byteBuffer. */
  public static String readString(ByteBuffer buffer) {
    int strLength = readInt(buffer);
    if (strLength < 0) {
      return null;
    }
    byte[] bytes = new byte[strLength];
    buffer.get(bytes, 0, strLength);
    return new String(bytes, 0, strLength);
  }

  /** read string from byteBuffer with user define length. */
  public static String readStringWithLength(ByteBuffer buffer, int length) {
    byte[] bytes = new byte[length];
    buffer.get(bytes, 0, length);
    return new String(bytes, 0, length);
  }

  public static ByteBuffer getByteBuffer(String s) {
    return ByteBuffer.wrap(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  public static ByteBuffer getByteBuffer(int i) {
    return ByteBuffer.allocate(4).putInt(0, i);
  }

  public static ByteBuffer getByteBuffer(long n) {
    return ByteBuffer.allocate(8).putLong(0, n);
  }

  public static ByteBuffer getByteBuffer(float f) {
    return ByteBuffer.allocate(4).putFloat(0, f);
  }

  public static ByteBuffer getByteBuffer(double d) {
    return ByteBuffer.allocate(8).putDouble(0, d);
  }

  public static ByteBuffer getByteBuffer(boolean i) {
    return ByteBuffer.allocate(1).put(i ? (byte) 1 : (byte) 0);
  }

  public static String readStringFromDirectByteBuffer(ByteBuffer buffer)
      throws CharacterCodingException {
    return java.nio.charset.StandardCharsets.UTF_8
        .newDecoder()
        .decode(buffer.duplicate())
        .toString();
  }

  /**
   * unlike InputStream.read(bytes), this method makes sure that you can read length bytes or reach
   * to the end of the stream.
   */
  public static byte[] readBytes(InputStream inputStream, int length) throws IOException {
    byte[] bytes = new byte[length];
    int offset = 0;
    int len = 0;
    while (bytes.length - offset > 0
        && (len = inputStream.read(bytes, offset, bytes.length - offset)) != -1) {
      offset += len;
    }
    return bytes;
  }

  public static Map<String, String> readMap(ByteBuffer buffer) {
    int length = readInt(buffer);
    Map<String, String> map = new HashMap<>(length);
    for (int i = 0; i < length; i++) {
      // key
      String key = readString(buffer);
      // value
      String value = readString(buffer);
      map.put(key, value);
    }
    return map;
  }

  /**
   * unlike InputStream.read(bytes), this method makes sure that you can read length bytes or reach
   * to the end of the stream.
   */
  public static byte[] readBytesWithSelfDescriptionLength(InputStream inputStream)
      throws IOException {
    int length = readInt(inputStream);
    return readBytes(inputStream, length);
  }

  /**
   * read bytes from byteBuffer, this method makes sure that you can read length bytes or reach to
   * the end of the buffer.
   *
   * <p>read a int + buffer
   */
  public static ByteBuffer readByteBufferWithSelfDescriptionLength(ByteBuffer buffer) {
    int byteLength = readInt(buffer);
    byte[] bytes = new byte[byteLength];
    buffer.get(bytes);
    ByteBuffer byteBuffer = ByteBuffer.allocate(byteLength);
    byteBuffer.put(bytes);
    byteBuffer.flip();
    return byteBuffer;
  }

  /** List&lt;Integer&gt;. */
  public static List<Integer> readIntegerList(InputStream inputStream) throws IOException {
    int size = readInt(inputStream);
    if (size <= 0) {
      return null;
    }

    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(readInt(inputStream));
    }

    return list;
  }

  /** read integer list with self define length. */
  public static List<Integer> readIntegerList(ByteBuffer buffer) {
    int size = readInt(buffer);
    if (size <= 0) {
      return null;
    }

    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(readInt(buffer));
    }
    return list;
  }

  /** read string list with self define length. */
  public static List<String> readStringList(InputStream inputStream) throws IOException {
    List<String> list = new ArrayList<>();
    int size = readInt(inputStream);

    for (int i = 0; i < size; i++) {
      list.add(readString(inputStream));
    }

    return list;
  }

  /** read string list with self define length. */
  public static List<String> readStringList(ByteBuffer buffer) {
    int size = readInt(buffer);
    if (size <= 0) {
      return null;
    }

    List<String> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(readString(buffer));
    }

    return list;
  }

  /** read string list with self define length. */
  public static List<Object> readObjectList(InputStream inputStream) throws IOException {
    List<Object> list = new ArrayList<>();
    int size = readInt(inputStream);

    for (int i = 0; i < size; i++) {
      list.add(readObject(inputStream));
    }

    return list;
  }

  public static void writeObject(Object value, OutputStream outputStream) {
    try {
      if (value instanceof Long) {
        outputStream.write(ClassSerializeId.LONG.ordinal());
        write((long) value, outputStream);
      } else if (value instanceof Double) {
        outputStream.write(ClassSerializeId.DOUBLE.ordinal());
        write((double) value, outputStream);
      } else if (value instanceof Integer) {
        outputStream.write(ClassSerializeId.INTEGER.ordinal());
        write((int) value, outputStream);
      } else if (value instanceof Float) {
        outputStream.write(ClassSerializeId.FLOAT.ordinal());
        write((float) value, outputStream);
      } else if (value instanceof String) {
        outputStream.write(ClassSerializeId.BINARY.ordinal());
        byte[] bytes = ((String) value).getBytes();
        write((int) bytes.length, outputStream);
        outputStream.write(bytes);
      } else if (value instanceof Boolean) {
        outputStream.write(ClassSerializeId.BOOLEAN.ordinal());
        outputStream.write(Boolean.TRUE.equals(value) ? 1 : 0);
      } else if (value == null) {
        outputStream.write(ClassSerializeId.NULL.ordinal());
      } else {
        outputStream.write(ClassSerializeId.STRING.ordinal());
        byte[] bytes = value.toString().getBytes();
        write((int) bytes.length, outputStream);
        outputStream.write(bytes);
      }
    } catch (IOException ignored) {
      // ignored
    }
  }

  public static Object readObject(InputStream inputstream) throws IOException {
    ClassSerializeId serializeId = ClassSerializeId.values()[inputstream.read()];
    switch (serializeId) {
      case BOOLEAN:
        return readInt(inputstream) == 1;
      case FLOAT:
        return readFloat(inputstream);
      case DOUBLE:
        return readDouble(inputstream);
      case LONG:
        return readLong(inputstream);
      case INTEGER:
        return readInt(inputstream);

      case NULL:
        return null;
      case BINARY:
      case STRING:
      default:
        int length = readInt(inputstream);
        byte[] bytes = readBytes(inputstream, length);
        return new java.lang.String(bytes);
    }
  }

  enum ClassSerializeId {
    LONG,
    DOUBLE,
    INTEGER,
    FLOAT,
    BINARY,
    BOOLEAN,
    STRING,
    NULL
  }
}
