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

package cn.edu.tsinghua.iotdb.benchmark.workload.reader;

import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GenerateCSVReader extends BasicReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateCSVReader.class);
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();
  private Iterator<String[]> iterator = null;

  public GenerateCSVReader(List<String> files) {
    super(files);
  }

  @Override
  public boolean hasNextBatch() {
    if (currentFileIndex < files.size()) {
      try {
        currentFileName = files.get(currentFileIndex);
        CSVReader csvReader =
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

  /** convert the cachedLines to Record list */
  @Override
  public Batch nextBatch() {
    String separator = File.separator;
    if (separator.equals("\\")) {
      separator = "\\\\";
    }
    String[] url = currentFileName.split(separator);
    String originDevice = url[url.length - 2];
    String device = MetaUtil.getDeviceName(originDevice);
    DeviceSchema deviceSchema = null;
    List<String> sensors = null;
    List<Record> records = new ArrayList<>();
    try {
      boolean firstLine = true;
      while (iterator.hasNext()) {
        String[] items = iterator.next();
        if (firstLine) {
          sensors = new ArrayList<>();
          for (int i = 1; i < items.length; i++) {
            sensors.add(items[i]);
          }
          deviceSchema =
              new DeviceSchema(
                  MetaUtil.getGroupNameByDeviceStr(originDevice), originDevice, sensors);
          firstLine = false;
          continue;
        }
        String[] values = iterator.next();
        long timestamp = Long.parseLong(values[0]);
        List<Object> recordValues = new ArrayList<>();
        for (int i = 1; i < values.length; i++) {
          switch (baseDataSchema.getSensorType(device, sensors.get(i - 1))) {
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
      LOGGER.error("Failed to read file");
    }
    return new Batch(deviceSchema, records);
  }
}
