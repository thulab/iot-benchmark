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

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.workload.SingletonWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IGenerateDataWorkload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Responsible for writing and querying generate data. The order and number of operation(e.g.
 * ingestion) are determined by OperationController. The specific query and written data are
 * generated by SyntheticDataWorkload
 */
public abstract class GenerateBaseClient extends Client implements Runnable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(GenerateBaseClient.class);
  /** meta schema */
  protected final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();
  /** synthetic workload */
  protected final IGenerateDataWorkload syntheticWorkload;
  /** singleton workload when IS_CLIENT_BIND = false */
  protected final SingletonWorkload singletonWorkload;
  /** Log related */
  protected final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  /** Loop Index, using for loop and log */
  protected long loopIndex;
  /** Insert Loop Index, using for data insertion */
  protected long insertLoopIndex;

  public GenerateBaseClient(
      int id,
      CountDownLatch countDownLatch,
      CyclicBarrier barrier,
      IGenerateDataWorkload workload) {
    super(id, countDownLatch, barrier);
    syntheticWorkload = workload;
    singletonWorkload = SingletonWorkload.getInstance();
    insertLoopIndex = 0;
  }

  @Override
  protected void doTest() {
    String currentThread = Thread.currentThread().getName();
    int actualDeviceFloor = (int) (config.getDEVICE_NUMBER() * config.getREAL_INSERT_RATE());
    actualDeviceFloor = MetaUtil.getDeviceId(actualDeviceFloor);

    if (!config.isIS_POINT_COMPARISON()) {
      // print current progress periodically
      service.scheduleAtFixedRate(
          () -> {
            String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / config.getLOOP());
            LOGGER.info("{} {}% syntheticWorkload is done.", currentThread, percent);
          },
          1,
          config.getLOG_PRINT_INTERVAL(),
          TimeUnit.SECONDS);
    }
    doOperations(actualDeviceFloor);
    service.shutdown();
  }

  /**
   * Do Operations
   *
   * @param actualDeviceFloor contains
   */
  protected abstract void doOperations(int actualDeviceFloor);
}
