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

import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.DeviceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.*;

public class GenerateDataDeviceClient extends GenerateBaseClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataDeviceClient.class);
  private int verificationBatchSize = config.getVERIFICATION_BATCH();
  private int now = 0;

  public GenerateDataDeviceClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
  }

  @Override
  protected void doTest() {
    try {
      for (int i = 0; i < config.getDEVICE_NUMBER() / config.getCLIENT_NUMBER() + 1; i++) {
        DeviceQuery deviceQuery = queryWorkLoad.getDeviceQuery();
        if (deviceQuery == null) {
          break;
        }
        deviceQuery.setLimit(verificationBatchSize);
        int number = dbWrapper.deviceTotalNumber(deviceQuery);
        ScheduledExecutorService pointService = Executors.newSingleThreadScheduledExecutor();
        String currentThread = Thread.currentThread().getName();
        // print current progress periodically
        pointService.scheduleAtFixedRate(
            () -> {
              int loop = now * verificationBatchSize;
              String percent = String.format("%.2f", loop * 100.0D / number);
              LOGGER.info(
                  "{} Loop {} ({}%) check for {} is done.",
                  currentThread, loop, percent, deviceQuery.getDeviceSchema().getDevice());
            },
            1,
            config.getLOG_PRINT_INTERVAL(),
            TimeUnit.SECONDS);
        for (now = 0; now * verificationBatchSize < number; now++) {
          DeviceQuery query = deviceQuery.getQueryWithOffset(now * verificationBatchSize);
          dbWrapper.deviceQuery(query);
        }
        pointService.shutdown();
      }
    } catch (SQLException | TsdbException sqlException) {
      LOGGER.error("Failed DeviceQuery: " + sqlException.getMessage());
    }
  }
}
