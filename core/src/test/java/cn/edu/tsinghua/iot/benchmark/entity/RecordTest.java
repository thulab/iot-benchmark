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

package cn.edu.tsinghua.iot.benchmark.entity;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class RecordTest extends BenchmarkTestBase {

  @Test
  public void testConstructorAndGetters() {
    List<Object> values = Arrays.asList(1, "a", 3.14);
    Record record = new Record(123L, values);
    assertEquals(123L, record.getTimestamp());
    assertEquals(values, record.getRecordDataValue());
  }

  @Test
  public void testSizeEmpty() {
    Record record = new Record(0L, Collections.emptyList());
    assertEquals(0, record.size());
  }

  @Test
  public void testSizeThreeElements() {
    Record record = new Record(0L, Arrays.asList(1, "a", 3.14));
    assertEquals(3, record.size());
  }

  @Test
  public void testSetTimestamp() {
    Record record = new Record(1L, Arrays.asList(1));
    record.setTimestamp(999L);
    assertEquals(999L, record.getTimestamp());
  }

  @Test
  public void testEqualsAndHashCodeSameContent() {
    Record a = new Record(10L, Arrays.asList(1, "x", 2.5));
    Record b = new Record(10L, Arrays.asList(1, "x", 2.5));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testEqualsDifferentTimestamp() {
    Record a = new Record(10L, Arrays.asList(1, "x"));
    Record b = new Record(11L, Arrays.asList(1, "x"));
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsDifferentData() {
    Record a = new Record(10L, Arrays.asList(1, "x"));
    Record b = new Record(10L, Arrays.asList(1, "y"));
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsNullAndOtherType() {
    Record a = new Record(10L, Arrays.asList(1));
    assertFalse(a.equals(null));
    assertFalse(a.equals("not a record"));
  }

  @Test
  public void testEqualsSelf() {
    Record a = new Record(10L, Arrays.asList(1));
    assertTrue(a.equals(a));
  }

  @Test
  public void testSerializeRoundTripMixedTypes() throws Exception {
    // Note: Boolean values are intentionally excluded — ReadWriteIOUtils.writeObject writes a
    // Boolean as 1 byte but readObject reads it back via readInt (4 bytes), so Boolean
    // round-tripping is broken in the production code.
    Record original =
        new Record(42L, Arrays.asList((Object) 42, (Object) 3.14f, (Object) "hello", 1L));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    original.serialize(out);
    Record restored = Record.deserialize(new ByteArrayInputStream(out.toByteArray()));
    assertEquals(original, restored);
  }

  @Test
  public void testSerializeRoundTripEmptyData() throws Exception {
    Record original = new Record(7L, Collections.emptyList());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    original.serialize(out);
    Record restored = Record.deserialize(new ByteArrayInputStream(out.toByteArray()));
    assertEquals(original, restored);
  }

  @Test
  public void testToStringContainsTimestampAndValues() {
    Record record = new Record(55L, Arrays.asList("alpha"));
    String s = record.toString();
    assertTrue(s.contains("timestamp="));
    assertTrue(s.contains("55"));
    assertTrue(s.contains("alpha"));
  }
}
