/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy;

import cn.edu.tsinghua.iot.benchmark.entity.Record;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@code SessionStrategy.convertTypeForBLOB} null handling.
 *
 * <p>The method rewrites BLOB columns from String to Binary and used to cast unconditionally, so a
 * null cell threw NPE and failed the whole batch - reachable from SESSION_BY_RECORD(s) whenever a
 * batch carries a null in a BLOB column. Mutation testing found nothing covered this: the suite
 * stayed green with the NPE reintroduced.
 */
public class SessionStrategyBlobNullTest {

  /**
   * {@code SessionStrategy} holds a static {@code Config}, whose {@code initInnerFunction()} reads
   * {@code function.xml} and calls {@code System.exit(0)} when it is missing - which silently kills
   * the surefire fork. Point the property at the repo's conf before the class is referenced.
   */
  static {
    System.setProperty(
        "benchmark-conf",
        Paths.get("..", "configuration", "conf").toAbsolutePath().normalize().toString());
  }

  @Test
  public void nullBlobCellStaysNull() {
    // Column 1 is a populated BLOB, column 0 a null BLOB, column 2 a non-BLOB.
    Record record = new Record(1L, new ArrayList<>(Arrays.asList(null, "blob-value", 1.5D)));
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.DOUBLE);

    List<Object> converted = SessionStrategy.convertTypeForBLOB(record, dataTypes);

    assertNull("a null BLOB cell must stay null", converted.get(0));
    assertNotNull("a non-null BLOB cell must still be converted", converted.get(1));
    assertTrue("a BLOB value becomes a Binary", converted.get(1) instanceof Binary);
    assertEquals("non-BLOB columns are untouched", 1.5D, converted.get(2));
  }

  @Test
  public void fullyNullBlobRecordIsHandled() {
    Record record = new Record(1L, new ArrayList<>(Arrays.asList(null, null)));
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.BLOB, TSDataType.BLOB);

    List<Object> converted = SessionStrategy.convertTypeForBLOB(record, dataTypes);

    assertNull(converted.get(0));
    assertNull(converted.get(1));
  }
}
