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
import cn.edu.tsinghua.iot.benchmark.entity.enums.ColumnCategory;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class SensorTest extends BenchmarkTestBase {

  @Test
  public void testTwoArgConstructorDefaultsToField() {
    Sensor sensor = new Sensor("s1", SensorType.DOUBLE);
    assertEquals("s1", sensor.getName());
    assertEquals(SensorType.DOUBLE, sensor.getSensorType());
    assertEquals(ColumnCategory.FIELD, sensor.getColumnCategory());
  }

  @Test
  public void testThreeArgConstructor() {
    Sensor sensor = new Sensor("s1", SensorType.INT64, ColumnCategory.TAG);
    assertEquals("s1", sensor.getName());
    assertEquals(SensorType.INT64, sensor.getSensorType());
    assertEquals(ColumnCategory.TAG, sensor.getColumnCategory());
  }

  @Test
  public void testSetters() {
    Sensor sensor = new Sensor();
    sensor.setName("renamed");
    sensor.setSensorType(SensorType.FLOAT);
    sensor.setColumnCategory(ColumnCategory.ATTRIBUTE);
    assertEquals("renamed", sensor.getName());
    assertEquals(SensorType.FLOAT, sensor.getSensorType());
    assertEquals(ColumnCategory.ATTRIBUTE, sensor.getColumnCategory());
  }

  @Test
  public void testEqualsAndHashCodeSameContent() {
    Sensor a = new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD);
    Sensor b = new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testEqualsDifferentName() {
    Sensor a = new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD);
    Sensor b = new Sensor("s2", SensorType.DOUBLE, ColumnCategory.FIELD);
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsDifferentSensorType() {
    Sensor a = new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD);
    Sensor b = new Sensor("s1", SensorType.INT64, ColumnCategory.FIELD);
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsDifferentColumnCategory() {
    Sensor a = new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD);
    Sensor b = new Sensor("s1", SensorType.DOUBLE, ColumnCategory.TAG);
    assertNotEquals(a, b);
  }

  @Test
  public void testEqualsNull() {
    Sensor a = new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD);
    assertFalse(a.equals(null));
  }

  @Test
  public void testEqualsDifferentClass() {
    Sensor a = new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD);
    assertFalse(a.equals("not a sensor"));
  }

  @Test
  public void testSerializeRoundTripDouble() throws Exception {
    Sensor original = new Sensor("s1", SensorType.DOUBLE, ColumnCategory.FIELD);
    assertEquals(original, roundTrip(original));
  }

  @Test
  public void testSerializeRoundTripInt32() throws Exception {
    Sensor original = new Sensor("s2", SensorType.INT32, ColumnCategory.TAG);
    assertEquals(original, roundTrip(original));
  }

  @Test
  public void testToStringReturnsName() {
    Sensor sensor = new Sensor("sensor_name", SensorType.DOUBLE);
    assertEquals("sensor_name", sensor.toString());
  }

  private Sensor roundTrip(Sensor sensor) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    sensor.serialize(out);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    return Sensor.deserialize(in);
  }
}
