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

package cn.edu.tsinghua.iotdb.benchmark.client.real;

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.workload.DataWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.QueryWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IDataWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IQueryWorkLoad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public abstract class RealBaseClient extends Client implements Runnable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(RealBaseClient.class);
  /** RealDataWorkload */
  protected final IDataWorkLoad dataWorkLoad;
  /** QueryWorkload */
  protected final IQueryWorkLoad queryWorkLoad;
  /** Log related */
  protected final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  /** Loop index while write or query */
  protected long loopIndex;
  /** Total loop of write or query */
  protected long loop;

  public RealBaseClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
    this.dataWorkLoad = DataWorkLoad.getInstance(id);
    this.queryWorkLoad = QueryWorkLoad.getInstance();
    this.loopIndex = 0;
    this.loop = this.dataWorkLoad.getBatchNumber();
    if (!config.isIS_SENSOR_TS_ALIGNMENT()) {
      this.loop *= config.getSENSOR_NUMBER();
    }
  }

  /** Do test */
  @Override
  protected void doTest() {
    String currentThread = Thread.currentThread().getName();

    // print current progress periodically
    service.scheduleAtFixedRate(
        () -> {
          String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / loop);
          LOGGER.info("{} {}% realDataWorkload is done.", currentThread, percent);
        },
        1,
        config.getLOG_PRINT_INTERVAL(),
        TimeUnit.SECONDS);
    doOperations();
    service.shutdown();
  }

  /** Do Operations */
  protected abstract void doOperations();
}
