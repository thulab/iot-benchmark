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
import cn.edu.tsinghua.iot.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import com.opencsv.CSVReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;

public class CSVDataReader extends DataReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVDataReader.class);
  private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
  private Iterator<String[]> iterator = null;

  public CSVDataReader(List<String> files) {
    super(files);
  }

  @Override
  public boolean hasNextBatch() {
    return (iterator != null && iterator.hasNext()) || changeFile();
  }

  /** convert the cachedLines to Record list */
  @Override
  public IBatch nextBatch() {
    String separator = File.separator;
    if (separator.equals("\\")) {
      separator = "\\\\";
    }
    String[] url = currentFileName.split(separator);
    String deviceName = url[url.length - 2];
    DeviceSchema deviceSchema = null;
    List<Sensor> sensors = null;
    List<Record> records = new ArrayList<>();
    try {
      boolean firstLine = true;
      while (iterator.hasNext() && records.size() < config.getBATCH_SIZE_PER_WRITE()) {
        if (firstLine) {
          String[] items = iterator.next();
          DeviceSchema originMetaSchema = metaDataSchema.getDeviceSchemaByName(deviceName);
          Map<String, Sensor> stringToSensorMap = new HashMap<>();
          for (Sensor sensor : originMetaSchema.getSensors()) {
            stringToSensorMap.put(sensor.getName(), sensor);
          }
          sensors = new ArrayList<>();
          for (int i = 1; i < items.length; i++) {
            sensors.add(stringToSensorMap.get(items[i]));
          }
          deviceSchema =
              new DeviceSchema(
                  MetaUtil.getGroupIdFromDeviceName(deviceName),
                  deviceName,
                  sensors,
                  MetaUtil.getTags(deviceName));
          firstLine = false;
          continue;
        }
        String[] values = iterator.next();
        if (values[0].equals("Sensor")) {
          LOGGER.warn("There is some thing wrong when read file.");
          System.exit(1);
        }
        long timestamp = Long.parseLong(values[0]);
        List<Object> recordValues = new ArrayList<>();
        for (int i = 1; i < values.length; i++) {
          switch (sensors.get(i - 1).getSensorType()) {
            case BOOLEAN:
              recordValues.add(Boolean.parseBoolean(values[i]));
              break;
            case INT32:
              recordValues.add(Integer.parseInt(values[i]));
              break;
            case INT64:
            case TIMESTAMP:
              recordValues.add(Long.parseLong(values[i]));
              break;
            case FLOAT:
              recordValues.add(Float.parseFloat(values[i]));
              break;
            case DOUBLE:
              recordValues.add(Double.parseDouble(values[i]));
              break;
            case TEXT:
            case STRING:
            case BLOB:
              recordValues.add(values[i]);
              break;
            case DATE:
              recordValues.add(LocalDate.parse(values[i]));
              break;
            default:
              LOGGER.error("Error Type");
          }
        }
        Record record = new Record(timestamp, recordValues);
        records.add(record);
      }
    } catch (Exception exception) {
      exception.printStackTrace();
      LOGGER.error("Failed to read file:" + exception.getMessage());
    }
    return new Batch(deviceSchema, records);
  }

  private boolean changeFile() {
    if (currentFileIndex < files.size()) {
      try {
        currentFileName = files.get(currentFileIndex);
        com.opencsv.CSVReader csvReader =
            new CSVReaderBuilder(
                    new BufferedReader(
                        new InputStreamReader(
                            new FileInputStream(new File(currentFileName)),
                            StandardCharsets.UTF_8)))
                .build();
        iterator = csvReader.iterator();
      } catch (IOException ioException) {
        LOGGER.error("Failed to read " + files.get(currentFileIndex));
      }
      currentFileIndex++;
      return true;
    }
    return false;
  }
}
