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

import cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateDataDeviceClient;
import cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateDataMixClient;
import cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateDataWriteClient;
import cn.edu.tsinghua.iotdb.benchmark.client.real.RealDataSetQueryClient;
import cn.edu.tsinghua.iotdb.benchmark.client.real.RealDataSetWriteClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.DataWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.QueryWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IDataWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IQueryWorkLoad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public abstract class Client implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  protected static Config config = ConfigDescriptor.getInstance().getConfig();
  protected final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

  /** The id of client */
  protected final int clientThreadId;
  /** RealDataWorkload */
  protected final IDataWorkLoad dataWorkLoad;
  /** QueryWorkload */
  protected final IQueryWorkLoad queryWorkLoad;
  /** Log related */
  protected final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  /** Tested DataBase */
  protected DBWrapper dbWrapper = null;
  /** Related Schema */
  protected final List<DeviceSchema> deviceSchemas;
  /** Related Schema Size */
  protected final int deviceSchemasSize;
  /** Measurement */
  protected Measurement measurement;
  /** Total number of loop */
  protected long totalLoop = 0;
  /** Loop Index, using for loop and log */
  protected long loopIndex = 0;

  /** Control the end of client */
  private final CountDownLatch countDownLatch;

  private final CyclicBarrier barrier;

  public Client(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    this.countDownLatch = countDownLatch;
    this.barrier = barrier;
    this.dataWorkLoad = DataWorkLoad.getInstance(id);
    this.queryWorkLoad = QueryWorkLoad.getInstance();
    this.clientThreadId = id;
    this.deviceSchemas = MetaDataSchema.getInstance().getDeviceSchemaByClientId(clientThreadId);
    this.deviceSchemasSize = deviceSchemas.size();
    this.measurement = new Measurement();
    initDBWrappers();
  }

  public static Client getInstance(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
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
   * Firstly init dbWrapper After all thread is finished(using barrier), then doTest After test,
   * count down latch
   */
  @Override
  public void run() {
    try {
      try {
        if (dbWrapper != null) {
          dbWrapper.init();
        }
        // wait for that all clients start test simultaneously
        barrier.await();

        String currentThread = Thread.currentThread().getName();

        // print current progress periodically
        service.scheduleAtFixedRate(
            () -> {
              String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / this.totalLoop);
              LOGGER.info("{} {}% workload is done.", currentThread, percent);
            },
            1,
            config.getLOG_PRINT_INTERVAL(),
            TimeUnit.SECONDS);

        doTest();
        service.shutdown();
      } catch (Exception e) {
        LOGGER.error("Unexpected error: ", e);
      } finally {
        try {
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
    return measurement;
  }

  /** Do test */
  protected abstract void doTest();

  /** Init DBWrapper */
  protected void initDBWrappers() {
    List<DBConfig> dbConfigs = new ArrayList<>();
    dbConfigs.add(config.getDbConfig());
    if (config.isIS_DOUBLE_WRITE()) {
      dbConfigs.add(config.getANOTHER_DBConfig());
    }
    dbWrapper = new DBWrapper(dbConfigs, measurement);
  }
}
