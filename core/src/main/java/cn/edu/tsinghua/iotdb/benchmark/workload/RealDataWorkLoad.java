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

package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import com.opencsv.CSVReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RealDataWorkLoad extends DataWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealDataWorkLoad.class);
  private DeviceSchema deviceSchema;
  private final Iterator<Record> iterator;
  private long batchNumber = 0;

  public RealDataWorkLoad(List<String> files) {
    List<Record> records = new ArrayList<>();
    for (String file : files) {
      Iterator<String[]> iterator = null;
      com.opencsv.CSVReader csvReader;
      try {
        csvReader =
            new CSVReaderBuilder(
                    new BufferedReader(
                        new InputStreamReader(
                            Files.newInputStream(Paths.get(file)), StandardCharsets.UTF_8)))
                .build();
        iterator = csvReader.iterator();
      } catch (IOException ioException) {
        LOGGER.error("Failed to read " + files);
        break;
      }

      String separator = File.separator;
      if (separator.equals("\\")) {
        separator = "\\\\";
      }
      String[] url = file.split(separator);
      String deviceName = url[url.length - 2];
      List<Sensor> sensors = null;
      try {
        boolean firstLine = true;
        while (iterator.hasNext()) {
          if (firstLine) {
            String[] items = iterator.next();
            DeviceSchema originMetaSchema = metaDataSchema.getDeviceSchemaByName(deviceName);
            Map<String, Sensor> stringSensorMap = new HashMap<>();
            for (Sensor sensor : originMetaSchema.getSensors()) {
              stringSensorMap.put(sensor.getName(), sensor);
            }
            sensors = new ArrayList<>();
            for (int i = 1; i < items.length; i++) {
              sensors.add(stringSensorMap.get(items[i]));
            }
            deviceSchema = new DeviceSchema("0", deviceName, sensors, config.getDEVICE_TAGS());
            firstLine = false;
            continue;
          }
          batchNumber++;
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
                recordValues.add(Long.parseLong(values[i]));
                break;
              case FLOAT:
                recordValues.add(Float.parseFloat(values[i]));
                break;
              case DOUBLE:
                recordValues.add(Double.parseDouble(values[i]));
                break;
              case TEXT:
                recordValues.add(values[i]);
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
      } finally {
        try {
          csvReader.close();
        } catch (Exception e) {
          LOGGER.error("Failed to close file:" + e.getMessage());
        }
      }
    }
    iterator = records.listIterator();
  }

  @Override
  public Batch getOneBatch() throws WorkloadException {
    int size = 0;
    List<Record> batchRecord = new ArrayList<>();
    while (iterator.hasNext() && size < config.getBATCH_SIZE_PER_WRITE()) {
      Record record = iterator.next();
      if (config.isIS_RECENT_QUERY()) {
        currentTimestamp = Math.max(currentTimestamp, record.getTimestamp());
      }
      batchRecord.add(record);
      size++;
    }
    if (deviceSchema != null) {
      return new Batch(deviceSchema, batchRecord);
    } else {
      return null;
    }
  }

  @Override
  public long getBatchNumber() {
    return batchNumber / config.getBATCH_SIZE_PER_WRITE();
  }
}
