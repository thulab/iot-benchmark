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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BatchTest extends BenchmarkTestBase {

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

  @Test
  public void testDefaultConstructor() {
    Batch batch = new Batch();
    assertEquals(0, batch.getRecords().size());
    assertTrue(batch.getRecords() instanceof LinkedList);
    assertEquals(null, batch.getDeviceSchema());
    assertEquals(-1, batch.getColIndex());
  }

  @Test
  public void testParamConstructor() {
    DeviceSchema schema =
        buildSchema(
            "d1",
            Arrays.asList(
                new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD),
                new Sensor("s2", SensorType.INT64, ColumnCategory.FIELD)));
    List<Record> records = buildRecords(2, 2);
    Batch batch = new Batch(schema, records);
    assertEquals(schema, batch.getDeviceSchema());
    assertEquals(records, batch.getRecords());
  }

  @Test
  public void testPointNumAllFields() {
    DeviceSchema schema =
        buildSchema(
            "d1",
            Arrays.asList(
                new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD),
                new Sensor("s2", SensorType.INT64, ColumnCategory.FIELD)));
    Batch batch = new Batch(schema, buildRecords(3, 2));
    assertEquals(6L, batch.pointNum());
  }

  @Test
  public void testPointNumExcludesTag() {
    DeviceSchema schema =
        buildSchema(
            "d1",
            Arrays.asList(
                new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD),
                new Sensor("s2", SensorType.INT64, ColumnCategory.TAG)));
    Batch batch = new Batch(schema, buildRecords(4, 2));
    assertEquals(4L, batch.pointNum());
  }

  @Test
  public void testPointNumAllTagsEmptyRecords() {
    DeviceSchema schema =
        buildSchema(
            "d1",
            Arrays.asList(
                new Sensor("s1", SensorType.DOUBLE, ColumnCategory.TAG),
                new Sensor("s2", SensorType.INT64, ColumnCategory.TAG)));
    Batch batch = new Batch(schema, new LinkedList<>());
    assertEquals(0L, batch.pointNum());
  }

  @Test
  public void testAddSchemaAndContentOverwrites() {
    DeviceSchema schemaA =
        buildSchema("d1", Arrays.asList(new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD)));
    DeviceSchema schemaB =
        buildSchema("d2", Arrays.asList(new Sensor("s2", SensorType.INT64, ColumnCategory.FIELD)));
    List<Record> recordsA = buildRecords(1, 1);
    List<Record> recordsB = buildRecords(3, 1);
    Batch batch = new Batch(schemaA, recordsA);
    batch.addSchemaAndContent(schemaB, recordsB);
    assertEquals(schemaB, batch.getDeviceSchema());
    assertEquals(recordsB, batch.getRecords());
  }

  @Test
  public void testColIndexGetterSetter() {
    Batch batch = new Batch();
    batch.setColIndex(5);
    assertEquals(5, batch.getColIndex());
  }

  @Test
  public void testHasNextAlwaysFalse() {
    Batch batch = new Batch();
    assertFalse(batch.hasNext());
  }

  @Test
  public void testNextThrowsUnsupported() {
    Batch batch = new Batch();
    try {
      batch.next();
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }

  @Test
  public void testResetDoesNotThrow() {
    Batch batch = new Batch();
    batch.reset();
  }

  @Test
  public void testEqualsAndHashCodeSameContent() {
    DeviceSchema schema =
        buildSchema("d1", Arrays.asList(new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD)));
    List<Record> r1 = buildRecords(2, 1);
    List<Record> r2 = buildRecords(2, 1);
    Batch a = new Batch(schema, r1);
    Batch b = new Batch(schema, r2);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testEqualsDifferentRecords() {
    DeviceSchema schema =
        buildSchema("d1", Arrays.asList(new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD)));
    Batch a = new Batch(schema, buildRecords(2, 1));
    Batch b = new Batch(schema, buildRecords(3, 1));
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsDifferentSchema() {
    DeviceSchema schemaA =
        buildSchema("d1", Arrays.asList(new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD)));
    DeviceSchema schemaB =
        buildSchema("d2", Arrays.asList(new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD)));
    List<Record> records = buildRecords(1, 1);
    Batch a = new Batch(schemaA, records);
    Batch b = new Batch(schemaB, records);
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsNullAndOtherType() {
    Batch a = new Batch();
    assertFalse(a.equals(null));
    assertFalse(a.equals("string"));
    assertFalse(a.equals(new MultiDeviceBatch(1)));
  }

  @Test
  public void testToStringContainsPrefix() {
    DeviceSchema schema =
        buildSchema("d1", Arrays.asList(new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD)));
    Batch batch = new Batch(schema, buildRecords(1, 1));
    assertTrue(batch.toString().startsWith("Batch{"));
  }
}
