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

import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.source.TsFileTableModelMapping;
import cn.edu.tsinghua.iot.benchmark.utils.FileUtils;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.DeviceTableModelWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileDataWriter extends DataWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileDataWriter.class);
  private static final long MEM_THRESHOLD = 32 * 1024 * 1024L;

  private final int clientId;
  private final Map<String, DeviceTableModelWriter> writers = new HashMap<>();

  public TsFileDataWriter(int clientId) {
    this.clientId = clientId;
  }

  @Override
  public boolean writeBatch(IBatch batch, long insertLoopIndex) throws Exception {
    while (true) {
      DeviceSchema deviceSchema = batch.getDeviceSchema();
      try {
        DeviceTableModelWriter writer = writerFor(deviceSchema);
        writer.write(buildTablet(deviceSchema, batch.getRecords()));
      } catch (Exception e) {
        LOGGER.error("Failed to write batch for device {}", batch.getDeviceSchema().getDevice(), e);
        return false;
      }
      if (!batch.hasNext()) {
        break;
      }
      batch.next();
    }
    batch.finishCheck();
    return true;
  }

  private DeviceTableModelWriter writerFor(DeviceSchema deviceSchema) throws IOException {
    String table = deviceSchema.getTable();
    DeviceTableModelWriter writer = writers.get(table);
    if (writer == null) {
      File dir = Paths.get(config.getFILE_PATH(), table).toFile();
      Files.createDirectories(dir.toPath());
      File file = new File(FileUtils.union(dir.getAbsolutePath(), "data_" + clientId + ".tsfile"));
      Files.deleteIfExists(file.toPath());
      writer = new DeviceTableModelWriter(file, tableSchemaOf(deviceSchema), MEM_THRESHOLD);
      writers.put(table, writer);
    }
    return writer;
  }

  /** Column order: FIELD sensors, then device_id TAG, then each tag key as TAG. */
  private TableSchema tableSchemaOf(DeviceSchema deviceSchema) {
    List<IMeasurementSchema> cols = new ArrayList<>();
    List<ColumnCategory> cats = new ArrayList<>();
    for (Sensor s : deviceSchema.getSensors()) {
      cols.add(
          new MeasurementSchema(
              s.getName(), TsFileTableModelMapping.toTsDataType(s.getSensorType())));
      cats.add(ColumnCategory.FIELD);
    }
    cols.add(new MeasurementSchema(TsFileTableModelMapping.DEVICE_ID_COLUMN, TSDataType.STRING));
    cats.add(ColumnCategory.TAG);
    for (String tagKey : deviceSchema.getTags().keySet()) {
      cols.add(new MeasurementSchema(tagKey, TSDataType.STRING));
      cats.add(ColumnCategory.TAG);
    }
    return new TableSchema(deviceSchema.getTable(), cols, cats);
  }

  private Tablet buildTablet(DeviceSchema deviceSchema, List<Record> records) {
    List<Sensor> sensors = deviceSchema.getSensors();
    List<String> names = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<ColumnCategory> cats = new ArrayList<>();
    for (Sensor s : sensors) {
      names.add(s.getName());
      types.add(TsFileTableModelMapping.toTsDataType(s.getSensorType()));
      cats.add(ColumnCategory.FIELD);
    }
    names.add(TsFileTableModelMapping.DEVICE_ID_COLUMN);
    types.add(TSDataType.STRING);
    cats.add(ColumnCategory.TAG);
    List<String> tagKeys = new ArrayList<>(deviceSchema.getTags().keySet());
    for (String tagKey : tagKeys) {
      names.add(tagKey);
      types.add(TSDataType.STRING);
      cats.add(ColumnCategory.TAG);
    }

    Tablet tablet = new Tablet(deviceSchema.getTable(), names, types, cats, records.size());
    for (int row = 0; row < records.size(); row++) {
      Record record = records.get(row);
      tablet.addTimestamp(row, record.getTimestamp());
      tablet.addValue(row, TsFileTableModelMapping.DEVICE_ID_COLUMN, deviceSchema.getDevice());
      for (String tagKey : tagKeys) {
        tablet.addValue(row, tagKey, deviceSchema.getTags().get(tagKey));
      }
      for (int i = 0; i < sensors.size(); i++) {
        addFieldValue(tablet, row, sensors.get(i), record.getRecordDataValue().get(i));
      }
    }
    tablet.setRowSize(records.size());
    return tablet;
  }

  private void addFieldValue(Tablet tablet, int row, Sensor sensor, Object value) {
    if (value == null) {
      return; // leave null
    }
    String col = sensor.getName();
    // value is Object; cast to the wrapper (not the primitive) before unboxing.
    switch (sensor.getSensorType()) {
      case BOOLEAN:
        tablet.addValue(row, col, (Boolean) value);
        break;
      case INT32:
        tablet.addValue(row, col, ((Number) value).intValue());
        break;
      case INT64:
      case TIMESTAMP:
        tablet.addValue(row, col, ((Number) value).longValue());
        break;
      case FLOAT:
        tablet.addValue(row, col, ((Number) value).floatValue());
        break;
      case DOUBLE:
        tablet.addValue(row, col, ((Number) value).doubleValue());
        break;
      default:
        tablet.addValue(row, col, String.valueOf(value));
    }
  }

  @Override
  public void close() {
    for (DeviceTableModelWriter writer : writers.values()) {
      try {
        writer.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close TsFile writer", e);
      }
    }
    writers.clear();
  }
}
