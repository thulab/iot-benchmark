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

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsFileSchemaReaderTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void enumeratesDevicesAndFieldSensors() throws Exception {
    File data = new File(folder.getRoot(), "data_0.tsfile");
    TsFileTestFixtures.writeSampleTable(data);
    config.setFILE_PATH(folder.getRoot().getAbsolutePath());

    Map<String, List<Sensor>> result = new TsFileSchemaReader().getDeviceSchemaList();

    assertEquals(2, result.size());
    assertTrue(result.containsKey("d_0"));
    assertTrue(result.containsKey("d_1"));
    List<Sensor> sensors = result.get("d_0");
    assertEquals(3, sensors.size());
    assertEquals("s_0", sensors.get(0).getName());
    assertEquals(SensorType.INT32, sensors.get(0).getSensorType());
    assertEquals(SensorType.DOUBLE, sensors.get(1).getSensorType());
    assertEquals(SensorType.BOOLEAN, sensors.get(2).getSensorType());
  }
}
