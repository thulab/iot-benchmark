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

import java.io.File;

import static org.junit.Assert.assertEquals;

public class FileUtilsTest {

  private static final String SEP = File.separator;

  @Test
  public void testUnionSingleSegment() {
    assertEquals("a", FileUtils.union("a"));
  }

  @Test
  public void testUnionTwoSegments() {
    assertEquals("a" + SEP + "b", FileUtils.union("a", "b"));
  }

  @Test
  public void testUnionMultipleSegments() {
    assertEquals("a" + SEP + "b" + SEP + "c" + SEP + "d", FileUtils.union("a", "b", "c", "d"));
  }

  @Test
  public void testUnionEmptyArray() {
    assertEquals("", FileUtils.union());
  }

  @Test
  public void testUnionWithEmptySegment() {
    assertEquals("a" + SEP + SEP + "b", FileUtils.union("a", "", "b"));
  }
}
