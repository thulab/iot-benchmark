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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class DeviceSummaryTest {

  @Test
  public void testConstructorAndGetters() {
    DeviceSummary summary = new DeviceSummary("d1", 100, 1000L, 2000L);
    assertEquals("d1", summary.getDevice());
    assertEquals(100, summary.getTotalLineNumber());
    assertEquals(1000L, summary.getMinTimeStamp());
    assertEquals(2000L, summary.getMaxTimeStamp());
  }

  @Test
  public void testEqualsSameContent() {
    DeviceSummary a = new DeviceSummary("d1", 100, 1000L, 2000L);
    DeviceSummary b = new DeviceSummary("d1", 100, 1000L, 2000L);
    assertEquals(a, b);
  }

  @Test
  public void testEqualsSelf() {
    DeviceSummary a = new DeviceSummary("d1", 100, 1000L, 2000L);
    assertEquals(a, a);
  }

  @Test
  public void testEqualsDifferentDevice() {
    DeviceSummary a = new DeviceSummary("d1", 100, 1000L, 2000L);
    DeviceSummary b = new DeviceSummary("d2", 100, 1000L, 2000L);
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsDifferentTotalLineNumber() {
    DeviceSummary a = new DeviceSummary("d1", 100, 1000L, 2000L);
    DeviceSummary b = new DeviceSummary("d1", 101, 1000L, 2000L);
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsDifferentMinTimeStamp() {
    DeviceSummary a = new DeviceSummary("d1", 100, 1000L, 2000L);
    DeviceSummary b = new DeviceSummary("d1", 100, 1001L, 2000L);
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsDifferentMaxTimeStamp() {
    DeviceSummary a = new DeviceSummary("d1", 100, 1000L, 2000L);
    DeviceSummary b = new DeviceSummary("d1", 100, 1000L, 2001L);
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsNull() {
    DeviceSummary a = new DeviceSummary("d1", 100, 1000L, 2000L);
    assertFalse(a.equals(null));
  }

  @Test
  public void testEqualsDifferentType() {
    DeviceSummary a = new DeviceSummary("d1", 100, 1000L, 2000L);
    assertFalse(a.equals("not a DeviceSummary"));
  }

  @Test
  public void testToStringContainsAllFields() {
    DeviceSummary a = new DeviceSummary("d1", 100, 1000L, 2000L);
    String str = a.toString();
    assertTrue(str.contains("d1"));
    assertTrue(str.contains("100"));
    assertTrue(str.contains("1000"));
    assertTrue(str.contains("2000"));
  }
}
