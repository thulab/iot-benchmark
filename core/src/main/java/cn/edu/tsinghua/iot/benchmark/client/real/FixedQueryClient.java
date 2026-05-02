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

package cn.edu.tsinghua.iot.benchmark.client.real;

import cn.edu.tsinghua.iot.benchmark.client.progress.TaskProgress;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class FixedQueryClient extends RealBaseClient {

  private static final int queryLoop = config.getQUERY_LOOP();

  public FixedQueryClient(
      int id, CountDownLatch countDownLatch, CyclicBarrier barrier, TaskProgress taskProgress) {
    super(id, countDownLatch, barrier, taskProgress);
  }

  /** Do Operations */
  @Override
  protected void doTest() {
    for (int i = 0; i < queryLoop; i++) {
      try {
        Status status = dbWrapper.deviceQuery(config.getFIXED_QUERY_SQL());
        long start = 0;
        if (config.getOP_MIN_INTERVAL() > 0) {
          start = System.currentTimeMillis();
        }

        if (!status.isOk()) {
          return;
        }

        double costTime = status.getTimeCost() / Math.pow(10, 6);
        LOGGER.debug(
            "sql cost time {} ms, sql is {}, result is {}",
            costTime,
            config.getFIXED_QUERY_SQL(),
            status.getRecords());
        taskProgress.incrementLoopIndex();

        if (config.getOP_MIN_INTERVAL() > 0) {
          long opMinInterval;
          opMinInterval = config.getOP_MIN_INTERVAL();
          long elapsed = System.currentTimeMillis() - start;
          if (elapsed < opMinInterval) {
            try {
              LOGGER.debug("[Client-{}] sleep {} ms.", clientThreadId, opMinInterval - elapsed);
              Thread.sleep(opMinInterval - elapsed);
            } catch (InterruptedException e) {
              LOGGER.error("Wait for next operation failed because ", e);
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Failed to query sql because ", e);
      }
    }
  }
}
