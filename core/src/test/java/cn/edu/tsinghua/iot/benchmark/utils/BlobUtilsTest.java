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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BlobUtilsTest {

  @Test
  public void testBytesToHexEmpty() {
    assertEquals("", BlobUtils.bytesToHex(new byte[0]));
  }

  @Test
  public void testBytesToHexTypicalSequence() {
    byte[] bytes = {(byte) 0x0A, (byte) 0xFF, (byte) 0x00, (byte) 0x7F};
    assertEquals("0AFF007F", BlobUtils.bytesToHex(bytes));
  }

  @Test
  public void testBytesToHexSingleByteZero() {
    assertEquals("00", BlobUtils.bytesToHex(new byte[] {(byte) 0x00}));
  }

  @Test
  public void testBytesToHexSingleByteFF() {
    assertEquals("FF", BlobUtils.bytesToHex(new byte[] {(byte) 0xFF}));
  }

  @Test
  public void testBytesToHexSingleByte10() {
    assertEquals("10", BlobUtils.bytesToHex(new byte[] {(byte) 0x10}));
  }

  @Test
  public void testStringToHexEmpty() {
    assertEquals("", BlobUtils.stringToHex(""));
  }

  @Test
  public void testStringToHexAscii() {
    // 'A'=0x41, 'B'=0x42, 'C'=0x43
    assertEquals("414243", BlobUtils.stringToHex("ABC"));
  }

  @Test
  public void testStringToHexChinese() {
    // "中" in UTF-8 is 0xE4 0xB8 0xAD
    assertEquals("E4B8AD", BlobUtils.stringToHex("中"));
  }
}
