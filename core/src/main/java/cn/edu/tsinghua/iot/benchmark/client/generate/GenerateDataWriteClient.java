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

package cn.edu.tsinghua.iot.benchmark.client.generate;

import cn.edu.tsinghua.iot.benchmark.entity.Batch;
import cn.edu.tsinghua.iot.benchmark.extern.DataWriter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/** This client is using by GenerateMode */
public class GenerateDataWriteClient extends GenerateBaseClient {
  private DataWriter dataWriter = DataWriter.getDataWriter();

  public GenerateDataWriteClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
  }

  /** Do Operations */
  @Override
  protected void doTest() {
    loop:
    for (loopIndex = 0; loopIndex < config.getLOOP(); loopIndex++) {
      if (!doGenerate()) {
        break loop;
      }
      if (isStop.get()) {
        break;
      }
    }
  }

  /** Do Ingestion Operation @Return when connect failed return false */
  private boolean doGenerate() {
    try {
      for (int i = 0; i < deviceSchemas.size(); i++) {
        int innerLoop =
            config.isIS_SENSOR_TS_ALIGNMENT() ? 1 : deviceSchemas.get(i).getSensors().size();
        for (int j = 0; j < innerLoop; j++) {
          Batch batch = dataWorkLoad.getOneBatch();
          if (checkBatch(batch)) {
            dataWriter.writeBatch(batch, insertLoopIndex);
          }
        }
      }
      insertLoopIndex++;
    } catch (Exception e) {
      LOGGER.error("Failed to insert one batch data because ", e);
    }
    return true;
  }

  @Override
  protected void initDBWrappers() {
    // do nothing
    this.totalLoop = config.getLOOP();
  }
}
