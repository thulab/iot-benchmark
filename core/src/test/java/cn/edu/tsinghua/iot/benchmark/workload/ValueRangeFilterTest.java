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

package cn.edu.tsinghua.iot.benchmark.workload;

import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ValueRangeFilterTest {

  private List<Sensor> buildSensors() {
    return Arrays.asList(new Sensor("s1", SensorType.DOUBLE), new Sensor("s2", SensorType.INT64));
  }

  @Test
  public void testThreeArgConstructor() {
    List<Sensor> sensors = buildSensors();
    ValueRangeFilter filter = new ValueRangeFilter(1.5, sensors, 9.5);
    assertEquals(Double.valueOf(1.5), filter.getMinValue());
    assertEquals(Double.valueOf(9.5), filter.getMaxValue());
    assertSame(sensors, filter.getSensors());
  }

  @Test
  public void testMinAndSensorsConstructorMaxIsNull() {
    List<Sensor> sensors = buildSensors();
    ValueRangeFilter filter = new ValueRangeFilter(1.5, sensors);
    assertEquals(Double.valueOf(1.5), filter.getMinValue());
    assertNull(filter.getMaxValue());
    assertSame(sensors, filter.getSensors());
  }

  @Test
  public void testSensorsAndMaxConstructorMinIsNull() {
    List<Sensor> sensors = buildSensors();
    ValueRangeFilter filter = new ValueRangeFilter(sensors, 9.5);
    assertNull(filter.getMinValue());
    assertEquals(Double.valueOf(9.5), filter.getMaxValue());
    assertSame(sensors, filter.getSensors());
  }
}
