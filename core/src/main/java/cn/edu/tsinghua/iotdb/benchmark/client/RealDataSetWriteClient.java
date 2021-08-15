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

import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.workload.IRealDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class RealDataSetWriteClient extends Client implements Runnable {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RealDataSetWriteClient.class);

  private final IRealDataWorkload realDataWorkload;
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  private long batchIndex = 0;

  public RealDataSetWriteClient(
      int id, CountDownLatch countDownLatch, CyclicBarrier barrier, IRealDataWorkload workload) {
    super(id, countDownLatch, barrier);
    realDataWorkload = workload;
  }

  @Override
  void doTest() {
    String currentThread = Thread.currentThread().getName();

    // print current progress periodically
    service.scheduleAtFixedRate(
        () -> {
          LOGGER.info("{} {} batch RealDataWorkload is done.", currentThread, batchIndex);
        },
        1,
        config.getLOG_PRINT_INTERVAL(),
        TimeUnit.SECONDS);

    while (true) {
      try {
        Batch batch = realDataWorkload.getOneBatch();
        if (batch == null) {
          break;
        }
        dbWrapper.insertOneBatch(batch);
        batchIndex++;
      } catch (DBConnectException e) {
        LOGGER.error("Failed to insert one batch data because ", e);
      } catch (Exception e) {
        LOGGER.error("Failed to insert one batch data because ", e);
      }
    }

    service.shutdown();
  }
}
