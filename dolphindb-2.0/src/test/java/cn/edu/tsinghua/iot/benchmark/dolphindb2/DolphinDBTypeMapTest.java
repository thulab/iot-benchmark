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

package cn.edu.tsinghua.iot.benchmark.dolphindb2;

import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DolphinDBTypeMapTest {

  static {
    if (System.getProperty("benchmark-conf") == null) {
      System.setProperty(
          "benchmark-conf", new java.io.File("../configuration/conf").getAbsolutePath());
    }
  }

  private final DolphinDB db = new DolphinDB(new DBConfig());

  @Test
  public void allSensorTypesMapToValidDolphinDBType() {
    assertEquals("BOOL", db.typeMap(SensorType.BOOLEAN));
    assertEquals("INT", db.typeMap(SensorType.INT32));
    assertEquals("LONG", db.typeMap(SensorType.INT64));
    assertEquals("FLOAT", db.typeMap(SensorType.FLOAT));
    assertEquals("DOUBLE", db.typeMap(SensorType.DOUBLE));
    assertEquals("STRING", db.typeMap(SensorType.TEXT));
    assertEquals("STRING", db.typeMap(SensorType.STRING));
    assertEquals("BLOB", db.typeMap(SensorType.BLOB));
    assertEquals("BLOB", db.typeMap(SensorType.OBJECT));
    assertEquals("TIMESTAMP", db.typeMap(SensorType.TIMESTAMP));
    assertEquals("DATE", db.typeMap(SensorType.DATE));
  }
}
