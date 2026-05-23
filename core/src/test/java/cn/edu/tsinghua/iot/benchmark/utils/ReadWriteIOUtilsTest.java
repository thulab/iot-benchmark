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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadWriteIOUtilsTest {

  @Test
  public void testIntRoundTrip() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(123456, out);
    assertEquals(123456, ReadWriteIOUtils.readInt(new ByteArrayInputStream(out.toByteArray())));
  }

  @Test
  public void testLongRoundTrip() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(9876543210L, out);
    assertEquals(
        9876543210L, ReadWriteIOUtils.readLong(new ByteArrayInputStream(out.toByteArray())));
  }

  @Test
  public void testFloatRoundTrip() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(3.5f, out);
    assertEquals(
        3.5f, ReadWriteIOUtils.readFloat(new ByteArrayInputStream(out.toByteArray())), 0.0f);
  }

  @Test
  public void testDoubleRoundTrip() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(2.718281828d, out);
    assertEquals(
        2.718281828d,
        ReadWriteIOUtils.readDouble(new ByteArrayInputStream(out.toByteArray())),
        0.0d);
  }

  @Test
  public void testShortRoundTrip() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReadWriteIOUtils.write((short) 4321, out);
    assertEquals(
        (short) 4321, ReadWriteIOUtils.readShort(new ByteArrayInputStream(out.toByteArray())));
  }

  @Test
  public void testBooleanRoundTrip() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(Boolean.TRUE, out);
    ReadWriteIOUtils.write(Boolean.FALSE, out);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    assertTrue(ReadWriteIOUtils.readBool(in));
    assertFalse(ReadWriteIOUtils.readBool(in));
  }

  @Test
  public void testStringRoundTrip() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReadWriteIOUtils.write("benchmark", out);
    assertEquals(
        "benchmark", ReadWriteIOUtils.readString(new ByteArrayInputStream(out.toByteArray())));
  }
}
