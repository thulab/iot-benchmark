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

package cn.edu.tsinghua.iot.benchmark.entity.Batch;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.ColumnCategory;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MultiDeviceBatchTest extends BenchmarkTestBase {

  private DeviceSchema buildSchema(String device, List<Sensor> sensors) {
    return new DeviceSchema(device, sensors, new HashMap<>());
  }

  private List<Record> buildRecords(int count, int valuesPerRecord) {
    List<Record> records = new LinkedList<>();
    for (int i = 0; i < count; i++) {
      List<Object> values = new ArrayList<>();
      for (int j = 0; j < valuesPerRecord; j++) {
        values.add("v" + j);
      }
      records.add(new Record(i, values));
    }
    return records;
  }

  private List<Sensor> twoFieldsOneTag() {
    return Arrays.asList(
        new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD),
        new Sensor("s2", SensorType.INT64, ColumnCategory.FIELD),
        new Sensor("s3", SensorType.TEXT, ColumnCategory.TAG));
  }

  @Test
  public void testAddSchemaAndContentThreeTimesAndInitialIndex() {
    MultiDeviceBatch batch = new MultiDeviceBatch(3);
    DeviceSchema s1 = buildSchema("d1", twoFieldsOneTag());
    DeviceSchema s2 = buildSchema("d2", twoFieldsOneTag());
    DeviceSchema s3 = buildSchema("d3", twoFieldsOneTag());
    List<Record> r1 = buildRecords(1, 3);
    List<Record> r2 = buildRecords(2, 3);
    List<Record> r3 = buildRecords(3, 3);
    batch.addSchemaAndContent(s1, r1);
    batch.addSchemaAndContent(s2, r2);
    batch.addSchemaAndContent(s3, r3);

    // index starts at 0
    assertEquals(s1, batch.getDeviceSchema());
    assertEquals(r1, batch.getRecords());
  }

  @Test
  public void testHasNextAndNextAdvancesIndex() {
    MultiDeviceBatch batch = new MultiDeviceBatch(3);
    DeviceSchema s1 = buildSchema("d1", twoFieldsOneTag());
    DeviceSchema s2 = buildSchema("d2", twoFieldsOneTag());
    DeviceSchema s3 = buildSchema("d3", twoFieldsOneTag());
    batch.addSchemaAndContent(s1, buildRecords(1, 3));
    batch.addSchemaAndContent(s2, buildRecords(1, 3));
    batch.addSchemaAndContent(s3, buildRecords(1, 3));

    assertTrue(batch.hasNext());
    batch.next();
    assertEquals(s2, batch.getDeviceSchema());
    assertTrue(batch.hasNext());
    batch.next();
    assertEquals(s3, batch.getDeviceSchema());
    // at last element, hasNext should be false
    assertFalse(batch.hasNext());
  }

  @Test
  public void testNextOutOfBoundsThrows() {
    MultiDeviceBatch batch = new MultiDeviceBatch(1);
    batch.addSchemaAndContent(buildSchema("d1", twoFieldsOneTag()), buildRecords(1, 3));
    // advance to index == size (1)
    batch.next();
    try {
      batch.next();
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
      // expected
    }
  }

  @Test
  public void testResetRestoresIndexToZero() {
    MultiDeviceBatch batch = new MultiDeviceBatch(2);
    DeviceSchema s1 = buildSchema("d1", twoFieldsOneTag());
    DeviceSchema s2 = buildSchema("d2", twoFieldsOneTag());
    batch.addSchemaAndContent(s1, buildRecords(1, 3));
    batch.addSchemaAndContent(s2, buildRecords(1, 3));
    batch.next();
    assertEquals(s2, batch.getDeviceSchema());
    batch.reset();
    assertEquals(s1, batch.getDeviceSchema());
  }

  @Test
  public void testGetRecordsReturnsCurrentIndex() {
    MultiDeviceBatch batch = new MultiDeviceBatch(2);
    List<Record> r1 = buildRecords(1, 3);
    List<Record> r2 = buildRecords(2, 3);
    batch.addSchemaAndContent(buildSchema("d1", twoFieldsOneTag()), r1);
    batch.addSchemaAndContent(buildSchema("d2", twoFieldsOneTag()), r2);
    assertEquals(r1, batch.getRecords());
    batch.next();
    assertEquals(r2, batch.getRecords());
  }

  @Test
  public void testSetColIndexNoopAndGetColIndexAlwaysMinusOne() {
    MultiDeviceBatch batch = new MultiDeviceBatch(1);
    batch.setColIndex(7);
    assertEquals(-1, batch.getColIndex());
    batch.setColIndex(0);
    assertEquals(-1, batch.getColIndex());
  }

  @Test
  public void testPointNum() {
    MultiDeviceBatch batch = new MultiDeviceBatch(3);
    // 2 FIELD sensors out of 3 (one TAG), 3 devices, each with 4 records
    batch.addSchemaAndContent(buildSchema("d1", twoFieldsOneTag()), buildRecords(4, 3));
    batch.addSchemaAndContent(buildSchema("d2", twoFieldsOneTag()), buildRecords(4, 3));
    batch.addSchemaAndContent(buildSchema("d3", twoFieldsOneTag()), buildRecords(4, 3));
    // 2 (FIELD) * 3 (recordLists size) * 4 (records.get(0).size()) = 24
    assertEquals(24L, batch.pointNum());
  }
}
