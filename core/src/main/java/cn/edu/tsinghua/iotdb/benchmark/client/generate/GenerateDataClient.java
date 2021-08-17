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

package cn.edu.tsinghua.iotdb.benchmark.client.generate;

import cn.edu.tsinghua.iotdb.benchmark.utils.FileUtils;
import cn.edu.tsinghua.iotdb.benchmark.workload.SyntheticDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Type;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/** This client is using by GenerateMode */
public class GenerateDataClient extends GenerateBaseClient {

  public GenerateDataClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier, new SyntheticDataWorkload(id));
  }

  /**
   * Do Operations
   *
   * @param actualDeviceFloor
   */
  @Override
  protected void doOperations(int actualDeviceFloor) {
    loop:
    for (loopIndex = 0; loopIndex < config.getLOOP(); loopIndex++) {
      if (!doGenerate(actualDeviceFloor)) {
        break loop;
      }
    }
  }

  /**
   * Do Ingestion Operation
   *
   * @param actualDeviceFloor @Return when connect failed return false
   */
  private boolean doGenerate(int actualDeviceFloor) {
    try {
      if (config.isIS_CLIENT_BIND()) {
        List<DeviceSchema> schemas = baseDataSchema.getThreadDeviceSchema(clientThreadId);
        if (config.isIS_SENSOR_TS_ALIGNMENT()) {
          // IS_CLIENT_BIND == true && IS_SENSOR_TS_ALIGNMENT = true
          for (DeviceSchema deviceSchema : schemas) {
            if (deviceSchema.getDeviceId() <= actualDeviceFloor) {
              Batch batch = syntheticWorkload.getOneBatch(deviceSchema, insertLoopIndex);
              writeBatch(batch);
            }
          }
          insertLoopIndex++;
        } else {
          // IS_CLIENT_BIND == true && IS_SENSOR_IS_ALIGNMENT = false
          DeviceSchema sensorSchema = null;
          List<String> sensorList = new ArrayList<String>();
          for (DeviceSchema deviceSchema : schemas) {
            if (deviceSchema.getDeviceId() <= actualDeviceFloor) {
              int colIndex = 0;
              for (String sensor : deviceSchema.getSensors()) {
                sensorList = new ArrayList<String>();
                sensorList.add(sensor);
                sensorSchema = (DeviceSchema) deviceSchema.clone();
                sensorSchema.setSensors(sensorList);
                Batch batch =
                    syntheticWorkload.getOneBatch(sensorSchema, insertLoopIndex, colIndex);
                batch.setColIndex(colIndex);
                Type colType = baseDataSchema.getSensorType(deviceSchema.getDevice(), sensor);
                batch.setColType(colType);
                writeBatch(batch);
                colIndex++;
                insertLoopIndex++;
              }
            }
          }
        }
      } else {
        // IS_CLIENT_BIND = false
        Batch batch = singletonWorkload.getOneBatch();
        if (batch.getDeviceSchema().getDeviceId() <= actualDeviceFloor) {
          writeBatch(batch);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to insert one batch data because ", e);
    }
    return true;
  }

  /**
   * Write batch into file
   *
   * @param batch
   */
  private void writeBatch(Batch batch) {
    String device = batch.getDeviceSchema().getDevice();
    try {
      Path dirFile = Paths.get(FileUtils.union(config.getFILE_PATH(), device));
      if (!Files.exists(dirFile)) {
        Files.createDirectories(dirFile);
      }
      Path dataFile =
          Paths.get(
              FileUtils.union(config.getFILE_PATH(), device, "batch_" + insertLoopIndex + ".txt"));
      Files.createFile(dataFile);
      List<String> sensors = batch.getDeviceSchema().getSensors();
      String sensorLine = String.join(" ", sensors);
      sensorLine = "Sensor " + sensorLine + "\n";
      Files.write(dataFile, sensorLine.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
      for (Record record : batch.getRecords()) {
        StringBuffer line = new StringBuffer(String.valueOf(record.getTimestamp()));
        for (String sensor : sensors) {
          if (batch.getColIndex() != -1) {
            line.append(" ").append(record.getRecordDataValue().get(0));
          } else {
            int index = Integer.valueOf(sensor.split("_")[1]);
            line.append(" ").append(record.getRecordDataValue().get(index));
          }
        }
        line.append("\n");
        Files.write(
            dataFile, line.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
      }
    } catch (IOException ioException) {
      LOGGER.error("Write batch Error!" + batch.toString());
    }
  }

  @Override
  protected void initDBWrapper() {
    dbWrapper = null;
  }
}
