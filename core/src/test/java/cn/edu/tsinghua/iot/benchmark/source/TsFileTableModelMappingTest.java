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

package cn.edu.tsinghua.iot.benchmark.source;

import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TsFileTableModelMappingTest {

  @Test
  public void mapsTsDataTypeToSensorTypeByName() {
    assertEquals(SensorType.INT32, TsFileTableModelMapping.toSensorType(TSDataType.INT32));
    assertEquals(SensorType.DOUBLE, TsFileTableModelMapping.toSensorType(TSDataType.DOUBLE));
    assertEquals(SensorType.BOOLEAN, TsFileTableModelMapping.toSensorType(TSDataType.BOOLEAN));
    assertEquals(SensorType.STRING, TsFileTableModelMapping.toSensorType(TSDataType.STRING));
    assertEquals(SensorType.TIMESTAMP, TsFileTableModelMapping.toSensorType(TSDataType.TIMESTAMP));
    assertEquals(SensorType.DATE, TsFileTableModelMapping.toSensorType(TSDataType.DATE));
  }

  @Test
  public void mapsSensorTypeToTsDataTypeByName() {
    assertEquals(TSDataType.INT64, TsFileTableModelMapping.toTsDataType(SensorType.INT64));
    assertEquals(TSDataType.FLOAT, TsFileTableModelMapping.toTsDataType(SensorType.FLOAT));
    assertEquals(TSDataType.TEXT, TsFileTableModelMapping.toTsDataType(SensorType.TEXT));
  }

  @Test
  public void unknownTsDataTypeFallsBackToText() {
    assertEquals(SensorType.TEXT, TsFileTableModelMapping.toSensorType(TSDataType.VECTOR));
  }
}
