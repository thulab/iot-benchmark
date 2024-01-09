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

import cn.edu.tsinghua.iot.benchmark.client.operation.OperationController;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.Interval;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GenerateDataMixClient extends GenerateBaseClient {

  /** Control operation according to OPERATION_PROPORTION */
  private final OperationController operationController;

  private final Random random = new Random(config.getDATA_SEED() + clientThreadId);

  public GenerateDataMixClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
    // TODO exclude control model
    this.operationController = new OperationController(id);
  }

  /** Do Operations */
  @Override
  protected void doTest() {
    ExecutorService executor = Executors.newFixedThreadPool(clientDeviceSchemas.size());
    for (int i = 0; i < clientDeviceSchemas.size(); i++) {
      executor.execute(
          () -> {
            for (loopIndex = 0; loopIndex < config.getLOOP(); loopIndex++) {
              try {
                long start = System.currentTimeMillis();
                IBatch batch = dataWorkLoad.getOneBatch();
                if (checkBatch(batch)) {
                  dbWrapper.insertOneBatchWithCheck(batch);
                  Interval interval = batch.getDeviceSchema().getInterval();
                  int opInterval = (int) (interval.getWriteIntervalLower());
                  if (interval.getWriteIntervalUpper() - interval.getWriteIntervalLower() > 0) {
                    opInterval +=
                        random.nextInt(
                            (int)
                                (interval.getWriteIntervalUpper()
                                    - interval.getWriteIntervalLower()));
                  }
                  if (opInterval > 0) {
                    long elapsed = System.currentTimeMillis() - start;
                    if (elapsed < opInterval) {
                      try {
                        LOGGER.debug(
                            "[Client-{}] sleep {} ms.", clientThreadId, opInterval - elapsed);
                        Thread.sleep(opInterval - elapsed);
                      } catch (InterruptedException e) {
                        LOGGER.error("Wait for next operation failed because ", e);
                      }
                    }
                  }
                }
              } catch (Exception e) {
                LOGGER.error("Failed to insert one batch data because ", e);
              }
              insertLoopIndex++;
            }
          });
    }
  }
}
