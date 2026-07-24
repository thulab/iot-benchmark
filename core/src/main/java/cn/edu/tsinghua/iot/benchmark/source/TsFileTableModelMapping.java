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

import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.List;

/** Mapping helpers between benchmark entities and TsFile table-model columns. */
public class TsFileTableModelMapping {

  /** Canonical TAG column that benchmark-generated TsFiles use to carry the device name. */
  public static final String DEVICE_ID_COLUMN = "device_id";

  private TsFileTableModelMapping() {}

  public static SensorType toSensorType(TSDataType type) {
    try {
      return SensorType.valueOf(type.name());
    } catch (IllegalArgumentException e) {
      return SensorType.TEXT;
    }
  }

  public static TSDataType toTsDataType(SensorType type) {
    try {
      return TSDataType.valueOf(type.name);
    } catch (IllegalArgumentException e) {
      return TSDataType.TEXT;
    }
  }

  /** FIELD columns of a table, in column order, as benchmark {@link Sensor}s. */
  public static List<Sensor> fieldSensors(TableSchema table) {
    List<IMeasurementSchema> cols = table.getColumnSchemas();
    List<ColumnCategory> cats = table.getColumnTypes();
    List<Sensor> sensors = new ArrayList<>();
    for (int i = 0; i < cols.size(); i++) {
      if (cats.get(i) == ColumnCategory.FIELD) {
        sensors.add(
            new Sensor(
                cols.get(i).getMeasurementName(),
                toSensorType(cols.get(i).getType()),
                cn.edu.tsinghua.iot.benchmark.entity.enums.ColumnCategory.FIELD));
      }
    }
    return sensors;
  }

  /** Names of TAG columns, in column order. */
  public static List<String> tagColumnNames(TableSchema table) {
    List<IMeasurementSchema> cols = table.getColumnSchemas();
    List<ColumnCategory> cats = table.getColumnTypes();
    List<String> tags = new ArrayList<>();
    for (int i = 0; i < cols.size(); i++) {
      if (cats.get(i) == ColumnCategory.TAG) {
        tags.add(cols.get(i).getMeasurementName());
      }
    }
    return tags;
  }
}
