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
import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public abstract class BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final double NANO_TO_SECOND = 1000000000.0d;

  protected ExecutorService schemaExecutorService =
      Executors.newFixedThreadPool(
          config.getCLIENT_NUMBER(), new NamedThreadFactory("SchemaClient"));
  protected ExecutorService executorService =
      Executors.newFixedThreadPool(config.getCLIENT_NUMBER(), new NamedThreadFactory("DataClient"));
  protected CountDownLatch schemaDownLatch = new CountDownLatch(config.getCLIENT_NUMBER());
  protected CyclicBarrier schemaBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());
  protected CountDownLatch dataDownLatch = new CountDownLatch(config.getCLIENT_NUMBER());
  protected CyclicBarrier dataBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());
  protected List<DataClient> dataClients = new ArrayList<>();
  protected List<SchemaClient> schemaClients = new ArrayList<>();
  protected Measurement baseModeMeasurement = new Measurement();
  protected long startTime = 0;
  private Timer middleMeasureTimer = new Timer("ShowResultPeriodically");

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
    for (DataClient client : dataClients) {
      executorService.submit(client);
    }
    setTimeLimitTask();
    setMiddleMeasureTask();
    startTime = System.nanoTime();
    executorService.shutdown();
    try {
      // wait for all dataClients finish test
      dataDownLatch.await();
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all threads finish.", e);
      Thread.currentThread().interrupt();
    }
    postCheck();
    middleMeasureTimer.cancel();
  }

  private void setTimeLimitTask() {
    if (config.getTEST_MAX_TIME() != 0) {
      TimerTask stopAllDataClient =
          new TimerTask() {
            @Override
            public void run() {
              LOGGER.info(
                  "It has been tested for {} ms, start to stop all dataClients.",
                  config.getTEST_MAX_TIME());
              dataClients.forEach(DataClient::stopClient);
            }
          };
      middleMeasureTimer.schedule(stopAllDataClient, config.getTEST_MAX_TIME());
    }
  }

  private void setMiddleMeasureTask() {
    TimerTask measure =
        new TimerTask() {
          @Override
          public void run() {
            List<Operation> operations;
            if (config.isIS_POINT_COMPARISON()) {
              operations = Collections.singletonList(Operation.DEVICE_QUERY);
            } else {
              operations = Operation.getNormalOperation();
            }
            middleMeasure(
                baseModeMeasurement,
                dataClients.stream().map(DataClient::getMeasurement),
                startTime,
                operations);
          }
        };
    middleMeasureTimer.schedule(
        measure, TimeUnit.SECONDS.toMillis(1), config.getRESULT_PRINT_INTERVAL() * 1000L);
  }

  protected abstract void postCheck();

  /** Clean up data */
  protected boolean cleanUpData(List<DBConfig> dbConfigs) {
    DBWrapper dbWrapper = new DBWrapper(dbConfigs);
    try {
      dbWrapper.init();
      try {
        dbWrapper.cleanup();
      } catch (TsdbException e) {
        LOGGER.error("Cleanup {} failed because ", config.getNET_DEVICE(), e);
        return false;
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
  protected boolean registerSchema() {
    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      SchemaClient schemaClient = new SchemaClient(i, schemaDownLatch, schemaBarrier);
      schemaClients.add(schemaClient);
    }
    for (SchemaClient schemaClient : schemaClients) {
      schemaExecutorService.submit(schemaClient);
    }
    startTime = System.nanoTime();
    schemaExecutorService.shutdown();
    try {
      // wait for all dataClients finish test
      schemaDownLatch.await();
      schemaClients.stream()
          .map(SchemaClient::getMeasurement)
          .forEach(baseModeMeasurement::mergeCreateSchemaFinishTime);
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
      Stream<Measurement> allClientsMeasurement,
      long startTime,
      List<Operation> operations) {
    measure(
        measurement,
        allClientsMeasurement,
        startTime,
        operations,
        "All dataClients finished. The final test result is: ",
        true);
  }

  protected static void middleMeasure(
      Measurement measurement,
      Stream<Measurement> allClientsMeasurement,
      long startTime,
      List<Operation> operations) {
    measure(
        measurement,
        allClientsMeasurement,
        startTime,
        operations,
        "The test is in progress. The current test result is: ",
        false);
  }

  private static void measure(
      Measurement measurement,
      Stream<Measurement> allClientsMeasurement,
      long startTime,
      List<Operation> operations,
      String prefix,
      boolean needPrintConf) {
    measurement.setElapseTime((System.nanoTime() - startTime) / NANO_TO_SECOND);
    // sum up all the measurements and calculate statistics
    measurement.resetMeasurementMaps();
    allClientsMeasurement.forEach(measurement::mergeMeasurement);
    // output results
    String showMeasurement = prefix;
    if (needPrintConf) {
      showMeasurement += measurement.getConfigsString();
    }
    if (config.isUSE_MEASUREMENT()) {
      // must call calculateMetrics() before using the Metrics
      try {
        measurement.calculateMetrics(operations);
        if (!operations.isEmpty()) {
          showMeasurement += measurement.getMeasurementsString(operations);
          showMeasurement += measurement.getMetricsString(operations);
        }
      } catch (IllegalArgumentException e) {
        LOGGER.error(
            "Failed to show metric, please check the relation between LOOP and OPERATION_PROPORTION",
            e);
        return;
      }
    }
    LOGGER.info(showMeasurement);
    if (config.isCSV_OUTPUT()) {
      measurement.outputCSV();
    }
  }
}
