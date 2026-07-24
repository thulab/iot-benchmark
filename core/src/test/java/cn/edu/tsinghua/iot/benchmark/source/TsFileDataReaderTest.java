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
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TsFileDataReaderTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void streamsBatchesGroupedByDevice() throws Exception {
    File data = new File(folder.getRoot(), "data_0.tsfile");
    TsFileTestFixtures.writeSampleTable(data);
    config.setFILE_PATH(folder.getRoot().getAbsolutePath());
    config.setBATCH_SIZE_PER_WRITE(100);
    // No MetaDataSchema registration needed: the reader derives sensors from the TsFile itself.

    TsFileDataReader reader =
        new TsFileDataReader(Collections.singletonList(data.getAbsolutePath()));

    Map<String, List<Long>> timesByDevice = new HashMap<>();
    int batchCount = 0;
    while (reader.hasNextBatch()) {
      IBatch batch = reader.nextBatch();
      batchCount++;
      DeviceSchema ds = batch.getDeviceSchema();
      List<Long> times = timesByDevice.computeIfAbsent(ds.getDevice(), k -> new ArrayList<>());
      for (Record r : batch.getRecords()) {
        times.add(r.getTimestamp());
        assertEquals(3, r.getRecordDataValue().size());
      }
    }
    assertFalse(reader.hasNextBatch());
    assertEquals(2, timesByDevice.size());
    assertEquals(java.util.Arrays.asList(50L, 60L), timesByDevice.get("d_0"));
    assertEquals(java.util.Arrays.asList(100L, 200L), timesByDevice.get("d_1"));
    assertTrue(batchCount >= 2);
  }
}
