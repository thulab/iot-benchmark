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

import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.DeviceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class GenerateDataDeviceClient extends GenerateBaseClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataDeviceClient.class);

  public GenerateDataDeviceClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
  }

  @Override
  protected void doTest() {
    try {
      for (int i = 0; i < config.getDEVICE_NUMBER() / config.getCLIENT_NUMBER() + 1; i++) {
        DeviceQuery deviceQuery = queryWorkLoad.getDeviceQuery();
        if (deviceQuery == null) {
          return;
        }
        LOGGER.info("Start Check Device: " + deviceQuery.getDeviceSchema().getDevice());
        dbWrapper.deviceQuery(deviceQuery);
        LOGGER.info("Finish Check Device: " + deviceQuery.getDeviceSchema().getDevice());
      }
    } catch (SQLException sqlException) {
      LOGGER.error("Failed DeviceQuery: " + sqlException.getMessage());
    }
  }
}
