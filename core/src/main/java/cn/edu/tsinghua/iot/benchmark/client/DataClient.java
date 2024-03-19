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

package cn.edu.tsinghua.iot.benchmark.client;

import cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataDeviceClient;
import cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataMixClient;
import cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataWriteClient;
import cn.edu.tsinghua.iot.benchmark.client.real.RealDataSetQueryClient;
import cn.edu.tsinghua.iot.benchmark.client.real.RealDataSetWriteClient;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iot.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.NamedThreadFactory;
import cn.edu.tsinghua.iot.benchmark.workload.DataWorkLoad;
import cn.edu.tsinghua.iot.benchmark.workload.QueryWorkLoad;
import cn.edu.tsinghua.iot.benchmark.workload.interfaces.IDataWorkLoad;
import cn.edu.tsinghua.iot.benchmark.workload.interfaces.IQueryWorkLoad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DataClient implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataClient.class);

  protected static Config config = ConfigDescriptor.getInstance().getConfig();
  protected final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

  /** The id of client */
  protected final int clientThreadId;
  /** RealDataWorkload */
  protected final IDataWorkLoad dataWorkLoad;
  /** QueryWorkload */
  protected final IQueryWorkLoad queryWorkLoad;
  /** Log related */
  protected final ScheduledExecutorService service;
  /** Tested DataBase */
  protected DBWrapper dbWrapper = null;
  /** Related Schema */
  protected final List<DeviceSchema> clientDeviceSchemas;
  /** Total number of loop */
  protected long totalLoop = 0;
  /** Loop Index, using for loop and log */
  protected long loopIndex = 0;

  /** Control the status */
  protected AtomicBoolean isStop = new AtomicBoolean(false);

  /** Control the end of client */
  private final CountDownLatch countDownLatch;

  private final CyclicBarrier barrier;

  public DataClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    this.countDownLatch = countDownLatch;
    this.barrier = barrier;
    this.dataWorkLoad = DataWorkLoad.getInstance(id);
    this.queryWorkLoad = QueryWorkLoad.getInstance(id);
    this.clientThreadId = id;
    this.clientDeviceSchemas =
        MetaDataSchema.getInstance().getDeviceSchemaByClientId(clientThreadId);
    this.service =
        Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("ShowWorkProgress-" + clientThreadId));
    initDBWrappers();
  }

  public static DataClient getInstance(
      int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    switch (config.getBENCHMARK_WORK_MODE()) {
      case TEST_WITH_DEFAULT_PATH:
        if (config.isIS_POINT_COMPARISON()) {
          return new GenerateDataDeviceClient(id, countDownLatch, barrier);
        } else {
          return new GenerateDataMixClient(id, countDownLatch, barrier);
        }
      case GENERATE_DATA:
        return new GenerateDataWriteClient(id, countDownLatch, barrier);
      case VERIFICATION_WRITE:
        return new RealDataSetWriteClient(id, countDownLatch, barrier);
      case VERIFICATION_QUERY:
        return new RealDataSetQueryClient(id, countDownLatch, barrier);
      default:
        LOGGER.warn("No need to create client" + config.getBENCHMARK_WORK_MODE());
        break;
    }
    return null;
  }

  /**
   * Firstly init dbWrapper After all thread is finished(using dataBarrier), then doTest After test,
   * count down latch
   */
  @Override
  public void run() {
    try {
      try {
        if (dbWrapper != null) {
          dbWrapper.init();
        }
        // wait for that all dataClients start test simultaneously
        barrier.await();

        String currentThread = Thread.currentThread().getName();

        if (!config.isIS_POINT_COMPARISON()) {
          // print current progress periodically
          service.scheduleAtFixedRate(
              () -> {
                String percent = String.format("%.2f", loopIndex * 100.0D / this.totalLoop);
                LOGGER.info("{} {}% workload is done.", currentThread, percent);
              },
              1,
              config.getLOG_PRINT_INTERVAL(),
              TimeUnit.SECONDS);
        }

        doTest();
      } catch (Exception e) {
        LOGGER.error("Unexpected error: ", e);
      } finally {
        service.shutdown();
        try {
          assert dbWrapper != null;
          dbWrapper.close();
        } catch (TsdbException e) {
          LOGGER.error("Close {} error: ", config.getDbConfig().getDB_SWITCH(), e);
        }
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  public Measurement getMeasurement() {
    return dbWrapper.getMeasurement();
  }

  /** Do test, Notice please use `isStop` parameters to control */
  protected abstract void doTest();

  /** Init DBWrapper */
  protected void initDBWrappers() {
    List<DBConfig> dbConfigs = new ArrayList<>();
    dbConfigs.add(config.getDbConfig());
    if (config.isIS_DOUBLE_WRITE()) {
      dbConfigs.add(config.getANOTHER_DBConfig());
    }
    dbWrapper = new DBWrapper(dbConfigs);
  }

  /** Stop client */
  public void stopClient() {
    this.isStop.set(true);
  }
}
