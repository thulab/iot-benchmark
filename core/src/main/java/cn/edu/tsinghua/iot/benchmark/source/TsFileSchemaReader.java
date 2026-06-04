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
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.DeviceTableModelReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TsFileSchemaReader extends SchemaReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileSchemaReader.class);

  @Override
  public Map<String, List<Sensor>> getDeviceSchemaList() {
    Map<String, List<Sensor>> result = new LinkedHashMap<>();
    for (File file : listTsFiles(new File(config.getFILE_PATH()))) {
      try (DeviceTableModelReader reader = new DeviceTableModelReader(file)) {
        for (TableSchema table : reader.getAllTableSchema()) {
          List<Sensor> sensors = TsFileTableModelMapping.fieldSensors(table);
          for (String device : devicesOf(reader, table, sensors)) {
            result.putIfAbsent(device, sensors);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Failed to read schema from {}", file, e);
      }
    }
    LOGGER.info("Total devices: {}", result.size());
    return result;
  }

  /** Distinct device names (device_id TAG value) present in one table. */
  private List<String> devicesOf(
      DeviceTableModelReader reader, TableSchema table, List<Sensor> sensors) throws Exception {
    List<String> tagCols = TsFileTableModelMapping.tagColumnNames(table);
    // The table query executor only emits rows when at least one FIELD column is selected, so
    // include the first field sensor alongside the TAG columns we actually read for the device
    // name.
    List<String> queryCols = new ArrayList<>(tagCols);
    if (!sensors.isEmpty()) {
      queryCols.add(sensors.get(0).getName());
    }
    List<String> devices = new ArrayList<>();
    String lastDevice = null;
    ResultSet rs = reader.query(table.getTableName(), queryCols, Long.MIN_VALUE, Long.MAX_VALUE);
    try {
      while (rs.next()) {
        String device = deviceName(rs, tagCols);
        if (!device.equals(lastDevice)) { // rows are grouped by device
          if (!devices.contains(device)) {
            devices.add(device);
          }
          lastDevice = device;
        }
      }
    } finally {
      rs.close();
    }
    return devices;
  }

  /** device_id TAG if present, else all TAG values joined by '.'. */
  static String deviceName(ResultSet rs, List<String> tagCols) {
    if (tagCols.contains(TsFileTableModelMapping.DEVICE_ID_COLUMN)) {
      return rs.getString(TsFileTableModelMapping.DEVICE_ID_COLUMN);
    }
    List<String> parts = new ArrayList<>();
    for (String tag : tagCols) {
      parts.add(rs.getString(tag));
    }
    return String.join(".", parts);
  }

  /** Recursively collect *.tsfile files under root (info.txt and others ignored). */
  public static List<File> listTsFiles(File root) {
    List<File> out = new ArrayList<>();
    File[] children = root.listFiles();
    if (children == null) {
      return out;
    }
    Arrays.sort(children);
    for (File f : children) {
      if (f.isDirectory()) {
        out.addAll(listTsFiles(f));
      } else if (f.getName().endsWith(".tsfile")) {
        out.add(f);
      }
    }
    return out;
  }
}
