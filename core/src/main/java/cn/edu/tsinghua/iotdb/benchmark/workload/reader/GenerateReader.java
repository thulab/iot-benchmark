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

import cn.edu.tsinghua.iotdb.benchmark.utils.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GenerateReader extends BasicReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateReader.class);
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

  public GenerateReader(List<String> files) {
    super(files);
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
      String line = "";
      boolean firstLine = true;
      while ((line = bufferedReader.readLine()) != null) {
        if (firstLine) {
          int firstIndex = line.indexOf(" ");
          line = line.substring(firstIndex + 1);
          sensors = new ArrayList<>(Arrays.asList(line.split(" ")));
          deviceSchema = new DeviceSchema(MetaUtil.getGroupNameByDeviceStr(originDevice), originDevice, sensors);
          firstLine = false;
          continue;
        }
        if (line.trim().length() == 0) {
          continue;
        }
        String[] values = line.split(" ");
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
