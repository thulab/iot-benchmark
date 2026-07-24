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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.DeviceTableModelReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsFileRoundTripRawTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void writesAndReadsBackGroupedByDevice() throws Exception {
    File f = new File(folder.getRoot(), "sample.tsfile");
    TsFileTestFixtures.writeSampleTable(f);
    assertTrue(f.length() > 0);

    try (DeviceTableModelReader reader = new DeviceTableModelReader(f)) {
      List<TableSchema> tables = reader.getAllTableSchema();
      assertEquals(1, tables.size());
      TableSchema ts = tables.get(0);
      assertEquals("t_0", ts.getTableName());
      assertEquals(1, ts.getTagColumnCnt());
      assertEquals("device_id", ts.getColumnSchemas().get(0).getMeasurementName());
      assertEquals(ColumnCategory.TAG, ts.getColumnTypes().get(0));

      ResultSet rs =
          reader.query(
              ts.getTableName(),
              Arrays.asList("device_id", "s_0", "s_1", "s_2"),
              Long.MIN_VALUE,
              Long.MAX_VALUE);
      List<String> seen = new ArrayList<>();
      List<Long> times = new ArrayList<>();
      while (rs.next()) {
        times.add(rs.getLong(1)); // column 1 == Time
        seen.add(rs.getString("device_id"));
      }
      rs.close();
      // grouped by device, time-ascending within device
      assertEquals(Arrays.asList("d_0", "d_0", "d_1", "d_1"), seen);
      assertEquals(Arrays.asList(50L, 60L, 100L, 200L), times);
    }
  }
}
