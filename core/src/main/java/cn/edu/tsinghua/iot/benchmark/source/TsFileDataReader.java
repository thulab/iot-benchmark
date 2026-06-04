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

import cn.edu.tsinghua.iot.benchmark.entity.Batch.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.DeviceTableModelReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TsFileDataReader extends DataReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileDataReader.class);

  private DeviceTableModelReader currentReader;
  private Iterator<TableSchema> tableIterator;
  private ResultSet currentRs;
  private List<Sensor> currentSensors; // FIELD columns as benchmark Sensors, in order
  private List<String> currentFieldNames; // FIELD column names in order
  private List<String> currentTagNames;
  private List<SensorType> currentFieldTypes;

  private Row pending; // one-row lookahead

  public TsFileDataReader(List<String> files) {
    super(files);
  }

  /** A buffered row carries its table's sensor list so a batch built later stays consistent. */
  private static final class Row {
    final String device;
    final long timestamp;
    final List<Object> values;
    final List<Sensor> sensors;

    Row(String device, long timestamp, List<Object> values, List<Sensor> sensors) {
      this.device = device;
      this.timestamp = timestamp;
      this.values = values;
      this.sensors = sensors;
    }
  }

  @Override
  public boolean hasNextBatch() {
    if (pending == null) {
      pending = advanceRow();
    }
    return pending != null;
  }

  @Override
  public IBatch nextBatch() {
    if (pending == null) {
      pending = advanceRow();
    }
    if (pending == null) {
      return null;
    }
    String device = pending.device;
    DeviceSchema deviceSchema = new DeviceSchema(device, pending.sensors, MetaUtil.getTags(device));

    List<Record> records = new ArrayList<>();
    while (pending != null
        && pending.device.equals(device)
        && records.size() < config.getBATCH_SIZE_PER_WRITE()) {
      records.add(new Record(pending.timestamp, pending.values));
      pending = advanceRow();
    }
    return new Batch(deviceSchema, records);
  }

  /** Pull the next row across tables/files; null when exhausted. */
  private Row advanceRow() {
    try {
      while (true) {
        if (currentRs != null && currentRs.next()) {
          long time = currentRs.getLong(1); // column 1 == Time
          String device = TsFileSchemaReader.deviceName(currentRs, currentTagNames);
          List<Object> values = new ArrayList<>(currentFieldNames.size());
          for (int i = 0; i < currentFieldNames.size(); i++) {
            values.add(readValue(currentRs, currentFieldNames.get(i), currentFieldTypes.get(i)));
          }
          return new Row(device, time, values, currentSensors);
        }
        if (!advanceTableOrFile()) {
          return null;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to read TsFile rows", e);
      return null;
    }
  }

  /** Move to the next table (within current file) or next file. Returns false when nothing left. */
  private boolean advanceTableOrFile() throws Exception {
    if (currentRs != null) {
      currentRs.close();
      currentRs = null;
    }
    while (true) {
      if (tableIterator != null && tableIterator.hasNext()) {
        TableSchema table = tableIterator.next();
        currentSensors = TsFileTableModelMapping.fieldSensors(table);
        currentFieldNames = new ArrayList<>();
        currentFieldTypes = new ArrayList<>();
        for (Sensor s : currentSensors) {
          currentFieldNames.add(s.getName());
          currentFieldTypes.add(s.getSensorType());
        }
        currentTagNames = TsFileTableModelMapping.tagColumnNames(table);
        List<String> queryCols = new ArrayList<>(currentTagNames);
        queryCols.addAll(currentFieldNames);
        currentRs =
            currentReader.query(table.getTableName(), queryCols, Long.MIN_VALUE, Long.MAX_VALUE);
        return true;
      }
      if (!openNextFile()) {
        return false;
      }
    }
  }

  private boolean openNextFile() throws Exception {
    if (currentReader != null) {
      currentReader.close();
      currentReader = null;
      tableIterator = null;
    }
    if (currentFileIndex >= files.size()) {
      return false;
    }
    currentFileName = files.get(currentFileIndex++);
    currentReader = new DeviceTableModelReader(new File(currentFileName));
    tableIterator = currentReader.getAllTableSchema().iterator();
    return true;
  }

  private static Object readValue(ResultSet rs, String col, SensorType type) {
    if (rs.isNull(col)) {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        return rs.getBoolean(col);
      case INT32:
        return rs.getInt(col);
      case INT64:
      case TIMESTAMP:
        return rs.getLong(col);
      case FLOAT:
        return rs.getFloat(col);
      case DOUBLE:
        return rs.getDouble(col);
      case DATE:
        return rs.getDate(col);
      case TEXT:
      case STRING:
      case BLOB:
      default:
        return rs.getString(col);
    }
  }
}
