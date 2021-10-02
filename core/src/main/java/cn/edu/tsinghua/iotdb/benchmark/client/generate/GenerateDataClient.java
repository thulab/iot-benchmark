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

import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.extern.DataWriter;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.SyntheticDataWorkload;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/** This client is using by GenerateMode */
public class GenerateDataClient extends GenerateBaseClient {
  private DataWriter dataWriter = DataWriter.getDataWriter();

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
        if (config.isIS_SENSOR_TS_ALIGNMENT()) {
          // IS_CLIENT_BIND == true && IS_SENSOR_TS_ALIGNMENT = true
          for (DeviceSchema deviceSchema : deviceSchemas) {
            if (deviceSchema.getDeviceId() <= actualDeviceFloor) {
              Batch batch = syntheticWorkload.getOneBatch(deviceSchema, insertLoopIndex);
              dataWriter.writeBatch(batch, insertLoopIndex);
            }
          }
          insertLoopIndex++;
        } else {
          // IS_CLIENT_BIND == true && IS_SENSOR_IS_ALIGNMENT = false
          DeviceSchema sensorSchema = null;
          List<String> sensorList = new ArrayList<String>();
          for (DeviceSchema deviceSchema : deviceSchemas) {
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
                SensorType colSensorType =
                    metaDataSchema.getSensorType(deviceSchema.getDevice(), sensor);
                batch.setColType(colSensorType);
                dataWriter.writeBatch(batch, insertLoopIndex);
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
          dataWriter.writeBatch(batch, insertLoopIndex);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to insert one batch data because ", e);
    }
    return true;
  }

  @Override
  protected void initDBWrappers() {
    // do nothing
  }
}
