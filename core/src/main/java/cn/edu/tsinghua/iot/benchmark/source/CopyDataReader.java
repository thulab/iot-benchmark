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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CopyDataReader extends DataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CopyDataReader.class);
  private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
  private Iterator<String[]> iterator = null;
  private long loopTimes = config.getLOOP();
  private IBatch batch = null;
  private long deltaTimeStamp = 0;
  private DeviceSchema deviceSchema = null;
  private List<Sensor> sensors = null;
  private List<Record> records = new ArrayList<>();

  public CopyDataReader(List<String> files) {
    // 在初始化的时候全部把数据全部读出来存储到workloads上，不用每次都去读取一遍数据
    super(files);
    currentFileName = files.get(0);
    String separator = File.separator;
    if (separator.equals("\\")) {
      separator = "\\\\";
    }
    String[] url = currentFileName.split(separator);
    String deviceName = url[url.length - 2];

    long begin = 0;
    long curtimestamp = 0;
    try {
      com.opencsv.CSVReader csvReader =
          new CSVReaderBuilder(
                  new BufferedReader(
                      new InputStreamReader(
                          new FileInputStream(new File(currentFileName)), StandardCharsets.UTF_8)))
              .build();
      iterator = csvReader.iterator();
      boolean firstLine = true;
      boolean secondLine = true;
      // TODO 自定义是否BATCH_SIZE
      while (iterator.hasNext() && records.size() < config.getBATCH_SIZE_PER_WRITE()) {
        if (firstLine) {
          String[] items = iterator.next();
          // TODO Optimize
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
                  MetaUtil.getTag(deviceName));
          firstLine = false;
          continue;
        }
        String[] values = iterator.next();
        if (values[0].equals("Sensor")) {
          LOGGER.warn("There is some thing wrong when read file.");
          System.exit(1);
        }
        curtimestamp = Long.parseLong(values[0]);
        if (secondLine) {
          begin = curtimestamp;
          secondLine = false;
        }
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
        Record record = new Record(curtimestamp, recordValues);
        records.add(record);
      }
    } catch (Exception exception) {
      exception.printStackTrace();
      LOGGER.error("Failed to read file:" + exception.getMessage());
    }
    batch = new Batch(deviceSchema, records);
    deltaTimeStamp = curtimestamp - begin;
  }

  @Override
  public boolean hasNextBatch() {
    return loopTimes != 0;
  }

  @Override
  public IBatch nextBatch() {
    loopTimes -= 1;
    // TODO add white noise to differ the Batch
    // We need to change the timestamp
    if (!config.isIS_ADD_ANOMALY()) {
      for (Record record : batch.getRecords()) {
        record.setTimestamp(record.getTimestamp() + deltaTimeStamp);
      }
      return batch;
    } else {
      List<Record> anomalyRecords = new ArrayList<>();
      // 特定长度的添加异常值
      int anomalyLength = (int) (config.getANOMALY_RATE() * records.size());
      List<Boolean> addAnomalies =
          new ArrayList<>(Collections.nCopies(records.size() - anomalyLength, false));
      List<Boolean> ones = new ArrayList<>(Collections.nCopies(anomalyLength, true));
      addAnomalies.addAll(ones);
      Collections.shuffle(addAnomalies);
      // TODO 目前异常仅限于翻倍
      int times = config.getANOMALY_TIMES();
      for (int r = 0; r < records.size(); r++) {
        Record record = records.get(r);
        record.setTimestamp(record.getTimestamp() + deltaTimeStamp);
        // 是否添加异常
        if (addAnomalies.get(r)) {
          List<Object> recordValues = record.getRecordDataValue();
          List<Object> anomalyValues = new ArrayList<>();
          for (int i = 0; i < sensors.size(); i++) {
            switch (sensors.get(i).getSensorType()) {
              case TEXT:
              case BOOLEAN:
                anomalyValues.add(recordValues.get(i));
                break;
              case INT32:
                anomalyValues.add((Integer) recordValues.get(i) * times);
                break;
              case INT64:
                anomalyValues.add((Long) recordValues.get(i) * times);
                break;
              case FLOAT:
                anomalyValues.add((Float) recordValues.get(i) * times);
                break;
              case DOUBLE:
                anomalyValues.add((Double) recordValues.get(i) * times);
                break;
              default:
                LOGGER.error("Error Type");
            }
          }
          anomalyRecords.add(new Record(record.getTimestamp(), anomalyValues));
        } else {
          anomalyRecords.add(record);
        }
      }
      return new Batch(deviceSchema, anomalyRecords);
    }
  }
}
