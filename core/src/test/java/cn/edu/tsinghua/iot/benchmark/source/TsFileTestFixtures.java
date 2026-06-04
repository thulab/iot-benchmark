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
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.DeviceTableModelWriter;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Test-only helper: writes small table-model TsFiles for reader tests. */
public class TsFileTestFixtures {

  public static final List<String> FIELD_NAMES = Arrays.asList("s_0", "s_1", "s_2");
  public static final List<TSDataType> FIELD_TYPES =
      Arrays.asList(TSDataType.INT32, TSDataType.DOUBLE, TSDataType.BOOLEAN);

  /**
   * Writes table t_0 with device_id TAG + 3 FIELD columns, 2 devices x 2 rows, into {@code file}.
   */
  public static void writeSampleTable(File file) throws Exception {
    List<IMeasurementSchema> cols = new ArrayList<>();
    cols.add(new MeasurementSchema("device_id", TSDataType.STRING));
    for (int i = 0; i < FIELD_NAMES.size(); i++) {
      cols.add(new MeasurementSchema(FIELD_NAMES.get(i), FIELD_TYPES.get(i)));
    }
    List<ColumnCategory> cats =
        Arrays.asList(
            ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD);
    TableSchema schema = new TableSchema("t_0", cols, cats);

    List<String> names = Arrays.asList("device_id", "s_0", "s_1", "s_2");
    List<TSDataType> types =
        Arrays.asList(TSDataType.STRING, TSDataType.INT32, TSDataType.DOUBLE, TSDataType.BOOLEAN);

    try (DeviceTableModelWriter w = new DeviceTableModelWriter(file, schema, 10 * 1024 * 1024L)) {
      Tablet t = new Tablet("t_0", names, types, cats, 4);
      // interleave d_1 before d_0 to prove the reader regroups by device
      addRow(t, 0, 100L, "d_1", 11, 1.1, true);
      addRow(t, 1, 50L, "d_0", 1, 0.1, false);
      addRow(t, 2, 200L, "d_1", 12, 1.2, false);
      addRow(t, 3, 60L, "d_0", 2, 0.2, true);
      t.setRowSize(4);
      w.write(t);
    }
  }

  private static void addRow(
      Tablet t, int row, long time, String dev, int s0, double s1, boolean s2) {
    t.addTimestamp(row, time);
    t.addValue(row, "device_id", dev);
    t.addValue(row, "s_0", s0);
    t.addValue(row, "s_1", s1);
    t.addValue(row, "s_2", s2);
  }
}
