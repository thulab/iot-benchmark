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

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/** DeviceID: 1 sensor: longitude, latitude */
public class TDriveReader extends BasicReader {

  private static Logger logger = LoggerFactory.getLogger(TDriveReader.class);
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

  private DeviceSchema deviceSchema;
  private List<String> sensors = new ArrayList<>();

  public TDriveReader(List<String> files) {
    super(files);
    sensors.add("longitude");
    sensors.add("latitude");
  }

  @Override
  public void init() {
    currentDeviceId = new File(currentFile).getName().replaceAll("\\.txt", "");
    deviceSchema =
        new DeviceSchema(
            calGroupIdStr(currentDeviceId, config.getGROUP_NUMBER()), currentDeviceId, sensors);
  }

  @Override
  public Batch nextBatch() {
    List<Record> records = new ArrayList<>();
    for (String line : cachedLines) {
      Record record = convertToRecord(line);
      if (record != null) {
        records.add(record);
      }
    }
    return new Batch(deviceSchema, records);
  }

  private Record convertToRecord(String line) {

    try {
      List<Object> fields = new ArrayList<>();

      String[] items = line.split(",");

      fields.add(Double.valueOf(items[2]));
      fields.add(Double.valueOf(items[3]));

      Date date = dateFormat.parse(items[1]);
      long time = date.getTime();

      return new Record(time, fields);
    } catch (Exception ignore) {
      ignore.printStackTrace();
    }
    return null;
  }
}
