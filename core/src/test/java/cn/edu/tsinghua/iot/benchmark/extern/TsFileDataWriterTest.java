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

package cn.edu.tsinghua.iot.benchmark.extern;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TsFileDataWriterTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void writesReadableTsFilePerTable() throws Exception {
    config.setFILE_PATH(folder.getRoot().getAbsolutePath());

    List<Sensor> sensors =
        Arrays.asList(new Sensor("s_0", SensorType.INT32), new Sensor("s_1", SensorType.DOUBLE));
    DeviceSchema d0 = new DeviceSchema("g_0", "t_0", "d_0", sensors, new HashMap<>());
    List<Record> recs = new ArrayList<>();
    recs.add(new Record(10L, Arrays.asList((Object) 1, 0.1)));
    recs.add(new Record(20L, Arrays.asList((Object) 2, 0.2)));

    TsFileDataWriter writer = new TsFileDataWriter(0);
    assertTrue(writer.writeBatch(new Batch(d0, recs), 0L));
    writer.close();

    // dir name is the resolved table name (DeviceSchema applies the table-name prefix), not "t_0"
    File tableDir = new File(folder.getRoot(), d0.getTable());
    File[] files = tableDir.listFiles((dir, name) -> name.endsWith(".tsfile"));
    assertTrue(files != null && files.length == 1);
    assertTrue(files[0].length() > 0);
  }
}
