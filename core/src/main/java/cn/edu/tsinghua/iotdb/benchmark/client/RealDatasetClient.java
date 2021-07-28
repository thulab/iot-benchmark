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

package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.RealDatasetWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class RealDatasetClient extends Client implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealDatasetClient.class);
  private RealDatasetWorkLoad workload;

  public RealDatasetClient(
      int id,
      CountDownLatch countDownLatch,
      Config config,
      List<String> files,
      CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
    workload = new RealDatasetWorkLoad(files, config);
  }

  @Override
  void doTest() {
    try {
      while (true) {
        Batch batch = workload.getOneBatch();
        if (batch == null) {
          break;
        }
        dbWrapper.insertOneBatch(batch);
      }
    } catch (Exception e) {
      LOGGER.error("RealDatasetClient do test failed because ", e);
    }
  }
}
