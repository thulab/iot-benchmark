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

package cn.edu.tsinghua.iot.benchmark.mode;

import cn.edu.tsinghua.iot.benchmark.client.DataClient;
import cn.edu.tsinghua.iot.benchmark.client.SchemaClient;
import cn.edu.tsinghua.iot.benchmark.client.TimeClient;
import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final double NANO_TO_SECOND = 1000000000.0d;

  protected ExecutorService schemaExecutorService =
      Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
  protected ExecutorService executorService =
      Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
  protected CountDownLatch schemaDownLatch = new CountDownLatch(config.getCLIENT_NUMBER());
  protected CyclicBarrier schemaBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());
  protected CountDownLatch dataDownLatch = new CountDownLatch(config.getCLIENT_NUMBER());
  protected CyclicBarrier dataBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());
  protected List<DataClient> dataClients = new ArrayList<>();
  protected List<SchemaClient> schemaClients = new ArrayList<>();
  protected Measurement measurement = new Measurement();
  protected long start = 0;

  protected abstract boolean preCheck();

  /** Start benchmark */
  public void run() {
    if (!preCheck()) {
      return;
    }
    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      DataClient client = DataClient.getInstance(i, dataDownLatch, dataBarrier);
      if (client == null) {
        return;
      }
      dataClients.add(client);
    }
    TimeClient timeClient = new TimeClient(dataClients);
    timeClient.start();
    for (DataClient client : dataClients) {
      executorService.submit(client);
    }
    start = System.nanoTime();
    executorService.shutdown();
    try {
      // wait for all dataClients finish test
      dataDownLatch.await();
      if (timeClient.isAlive()) {
        timeClient.interrupt();
      }
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all threads finish.", e);
      Thread.currentThread().interrupt();
    }
    postCheck();
  }

  protected abstract void postCheck();

  /** Clean up data */
  protected boolean cleanUpData(List<DBConfig> dbConfigs, Measurement measurement) {
    DBWrapper dbWrapper = new DBWrapper(dbConfigs, measurement);
    try {
      dbWrapper.init();
      if (config.isIS_DELETE_DATA()) {
        try {
          dbWrapper.cleanup();
        } catch (TsdbException e) {
          LOGGER.error("Cleanup {} failed because ", config.getNET_DEVICE(), e);
          return false;
        }
      }
    } catch (TsdbException e) {
      LOGGER.error("Initialize {} failed because ", config.getNET_DEVICE(), e);
      return false;
    } finally {
      try {
        dbWrapper.close();
      } catch (TsdbException e) {
        LOGGER.error("Close {} failed because ", config.getNET_DEVICE(), e);
      }
    }
    return true;
  }

  /** Register schema */
  protected boolean registerSchema(Measurement measurement) {
    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      SchemaClient schemaClient = new SchemaClient(i, measurement, schemaDownLatch, schemaBarrier);
      schemaClients.add(schemaClient);
    }
    for (SchemaClient schemaClient : schemaClients) {
      schemaExecutorService.submit(schemaClient);
    }
    start = System.nanoTime();
    schemaExecutorService.shutdown();
    try {
      // wait for all dataClients finish test
      schemaDownLatch.await();
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all threads finish.", e);
      Thread.currentThread().interrupt();
    }
    LOGGER.info("Registering schema successful!");
    return true;
  }

  /** Save measure */
  protected static void finalMeasure(
      Measurement measurement,
      List<Measurement> threadsMeasurements,
      long st,
      List<DataClient> clients,
      List<Operation> operations) {
    long en = System.nanoTime();
    LOGGER.info("All dataClients finished.");
    // sum up all the measurements and calculate statistics
    measurement.setElapseTime((en - st) / NANO_TO_SECOND);
    for (DataClient client : clients) {
      threadsMeasurements.add(client.getMeasurement());
    }
    for (Measurement m : threadsMeasurements) {
      measurement.mergeMeasurement(m);
    }
    // output results
    measurement.showConfigs();
    if (config.isUSE_MEASUREMENT()) {
      // must call calculateMetrics() before using the Metrics
      try {
        measurement.calculateMetrics(operations);
        if (operations.size() != 0) {
          measurement.showMeasurements(operations);
          measurement.showMetrics(operations);
        }
      } catch (IllegalArgumentException e) {
        LOGGER.error(
            "Failed to show metric, please check the relation between LOOP and OPERATION_PROPORTION");
        return;
      }
    }
    if (config.isCSV_OUTPUT()) {
      measurement.outputCSV();
    }
  }
}
