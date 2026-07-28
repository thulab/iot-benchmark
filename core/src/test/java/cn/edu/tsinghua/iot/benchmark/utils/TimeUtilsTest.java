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

import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimeUtilsTest {

  @Test
  public void testConvertDateStrToTimestamp() {
    String dateStr = "2024-01-01T00:00:00Z";
    long expected = new DateTime(dateStr).getMillis();
    assertEquals(expected, TimeUtils.convertDateStrToTimestamp(dateStr));
  }

  @Test
  public void testGetTimestampConstMs() {
    assertEquals(1L, TimeUtils.getTimestampConst("ms"));
  }

  @Test
  public void testGetTimestampConstUs() {
    assertEquals(1000L, TimeUtils.getTimestampConst("us"));
  }

  @Test
  public void testGetTimestampConstNs() {
    assertEquals(1_000_000L, TimeUtils.getTimestampConst("ns"));
  }

  @Test
  public void testGetTimestampConstFallback() {
    // unknown precision falls into the else branch and returns 1_000_000
    assertEquals(1_000_000L, TimeUtils.getTimestampConst("foo"));
  }

  @Test
  public void testConvertToSecondsMs() {
    assertEquals(1.0, TimeUtils.convertToSeconds(1000L, "ms"), 1e-9);
  }

  @Test
  public void testConvertToSecondsUs() {
    assertEquals(1.0, TimeUtils.convertToSeconds(1_000_000L, "us"), 1e-9);
  }

  @Test
  public void testConvertToSecondsNs() {
    assertEquals(1.0, TimeUtils.convertToSeconds(1_000_000_000L, "ns"), 1e-9);
  }
}
