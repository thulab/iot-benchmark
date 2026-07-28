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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BytesUtilsTest {

  @Test
  public void testIntRoundTrip() {
    for (int v : new int[] {0, 1, -1, 42, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
      assertEquals(v, BytesUtils.bytesToInt(BytesUtils.intToBytes(v)));
    }
  }

  @Test
  public void testLongRoundTrip() {
    for (long v : new long[] {0L, 1L, -1L, 123456789L, Long.MAX_VALUE, Long.MIN_VALUE}) {
      assertEquals(v, BytesUtils.bytesToLong(BytesUtils.longToBytes(v)));
    }
  }

  @Test
  public void testFloatRoundTrip() {
    for (float v : new float[] {0f, 1.5f, -2.5f, Float.MAX_VALUE, Float.MIN_VALUE}) {
      assertEquals(v, BytesUtils.bytesToFloat(BytesUtils.floatToBytes(v)), 0.0f);
    }
  }

  @Test
  public void testDoubleRoundTrip() {
    for (double v : new double[] {0d, 3.14159d, -2.71828d, Double.MAX_VALUE, Double.MIN_VALUE}) {
      assertEquals(v, BytesUtils.bytesToDouble(BytesUtils.doubleToBytes(v)), 0.0d);
    }
  }

  @Test
  public void testShortRoundTrip() {
    for (short v : new short[] {0, 1, -1, 12345, Short.MAX_VALUE, Short.MIN_VALUE}) {
      assertEquals(v, BytesUtils.bytesToShort(BytesUtils.shortToBytes(v)));
    }
  }

  @Test
  public void testBoolRoundTrip() {
    assertTrue(BytesUtils.bytesToBool(BytesUtils.boolToBytes(true)));
    assertFalse(BytesUtils.bytesToBool(BytesUtils.boolToBytes(false)));
    assertTrue(BytesUtils.byteToBool(BytesUtils.boolToByte(true)));
    assertFalse(BytesUtils.byteToBool(BytesUtils.boolToByte(false)));
  }

  @Test
  public void testStringRoundTrip() {
    for (String v : new String[] {"", "hello", "时序数据库", "a b\tc"}) {
      assertEquals(v, BytesUtils.bytesToString(BytesUtils.stringToBytes(v)));
    }
  }

  @Test
  public void testTwoBytesRoundTrip() {
    // values < 2^15 so signed/unsigned interpretation cannot diverge
    for (int v : new int[] {0, 1, 255, 256, 30000}) {
      assertEquals(v, BytesUtils.twoBytesToInt(BytesUtils.intToTwoBytes(v)));
    }
  }

  @Test
  public void testConcatAndSubBytes() {
    byte[] a = {1, 2, 3};
    byte[] b = {4, 5};
    byte[] c = BytesUtils.concatByteArray(a, b);
    assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, c);
    assertArrayEquals(new byte[] {2, 3, 4}, BytesUtils.subBytes(c, 1, 3));
    assertNull("out-of-range returns null", BytesUtils.subBytes(c, 3, 10));
    assertNull("non-positive length returns null", BytesUtils.subBytes(c, 0, 0));
  }

  @Test
  public void testSetGetIntBit() {
    int data = BytesUtils.setIntN(0, 4, 1);
    assertEquals(16, data);
    assertEquals(1, BytesUtils.getIntN(data, 4));
    assertEquals(0, BytesUtils.getIntN(data, 3));
    assertEquals(0, BytesUtils.getIntN(BytesUtils.setIntN(data, 4, 0), 4));
  }
}
