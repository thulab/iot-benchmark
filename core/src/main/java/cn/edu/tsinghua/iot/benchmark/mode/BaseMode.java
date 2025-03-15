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
import cn.edu.tsinghua.iot.benchmark.constant.ThreadName;
import cn.edu.tsinghua.iot.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iot.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public abstract class BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(
          2, new NamedThreadFactory(ThreadName.SHOW_RESULT_PERIODICALLY.getName()));
  private static final ScheduledExecutorService printService =
      Executors.newSingleThreadScheduledExecutor(
          new NamedThreadFactory(ThreadName.SHOW_WORK_PROCESS.getName()));

  private static final double NANO_TO_SECOND = 1000000000.0d;
  private static final String RESULT_ITEM = "%-35s";
  private static final String LATENCY_ITEM = "%-80s";

  protected ExecutorService schemaExecutorService =
      Executors.newFixedThreadPool(
          config.getSCHEMA_CLIENT_NUMBER(),
          new NamedThreadFactory(ThreadName.SCHEMA_CLIENT_THREAD.getName()));
  protected ExecutorService executorService =
      Executors.newFixedThreadPool(
          config.getDATA_CLIENT_NUMBER(),
          new NamedThreadFactory(ThreadName.DATA_CLIENT_THREAD.getName()));
  protected CountDownLatch schemaDownLatch = new CountDownLatch(config.getSCHEMA_CLIENT_NUMBER());
  protected CyclicBarrier schemaBarrier = new CyclicBarrier(config.getSCHEMA_CLIENT_NUMBER());
  protected CountDownLatch dataDownLatch = new CountDownLatch(config.getDATA_CLIENT_NUMBER());

  public static HashMap<String, AtomicLong> threadNameLoopIndexMap =
      new HashMap<>(config.getDATA_CLIENT_NUMBER());
  protected static CyclicBarrier dataBarrier;
  protected List<DataClient> dataClients = new ArrayList<>();
  protected List<SchemaClient> schemaClients = new ArrayList<>();
  protected Measurement baseModeMeasurement = new Measurement();
  protected long startTime = 0;

  protected abstract boolean preCheck();

  /** Start benchmark */
  public void run() {
    if (!preCheck()) {
      return;
    }
    dataBarrier =
        new CyclicBarrier(
            config.getDATA_CLIENT_NUMBER(),
            () -> {
              printService.scheduleAtFixedRate(
                  () -> {
                    if (!config.isIS_POINT_COMPARISON()) {
                      for (Map.Entry<String, AtomicLong> entry :
                          threadNameLoopIndexMap.entrySet()) {
                        String percent =
                            String.format(
                                "%.2f", (entry.getValue().get() * 100.0D) / config.getLOOP());
                        LOGGER.info("{} {}% workload is done.", entry.getKey(), percent);
                      }
                    }
                  },
                  1,
                  config.getLOG_PRINT_INTERVAL(),
                  TimeUnit.SECONDS);
            });

    for (int i = 0; i < config.getDATA_CLIENT_NUMBER(); i++) {
      threadNameLoopIndexMap.put(
          ThreadName.DATA_CLIENT_THREAD.getName() + "-" + i, new AtomicLong(0));
      DataClient client =
          DataClient.getInstance(
              i,
              dataDownLatch,
              dataBarrier,
              threadNameLoopIndexMap.get(ThreadName.DATA_CLIENT_THREAD.getName() + "-" + i));
      if (client == null) {
        return;
      }
      dataClients.add(client);
    }
    for (DataClient client : dataClients) {
      executorService.submit(client);
    }
    setTimeLimitScheduler();
    if (config.getRESULT_PRINT_INTERVAL() != 0) {
      setMiddleMeasureScheduler();
    }
    startTime = System.nanoTime();
    executorService.shutdown();
    try {
      // wait for all dataClients finish test
      dataDownLatch.await();
      printService.shutdown();
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all threads finish.", e);
      Thread.currentThread().interrupt();
    }
    postCheck();
    printSqlStatements();
    scheduler.shutdownNow();
  }

  private void setTimeLimitScheduler() {
    if (config.getTEST_MAX_TIME() != 0) {
      scheduler.schedule(
          () -> {
            try {
              LOGGER.info(
                  "It has been tested for {} ms, start to stop all dataClients.",
                  config.getTEST_MAX_TIME());
              dataClients.forEach(DataClient::stopClient);
            } catch (Exception e) {
              LOGGER.error("Exception occurred during stopping data clients:", e);
            }
          },
          config.getTEST_MAX_TIME(),
          TimeUnit.MILLISECONDS);
    }
  }

  void setMiddleMeasureScheduler() {
    scheduler.scheduleAtFixedRate(
        () -> {
          try {
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
          } catch (Exception e) {
            LOGGER.error("Exception occurred during print measurement:", e);
          }
        },
        config.getRESULT_PRINT_INTERVAL(),
        config.getRESULT_PRINT_INTERVAL(),
        TimeUnit.SECONDS);
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
    for (int i = 0; i < config.getSCHEMA_CLIENT_NUMBER(); i++) {
      SchemaClient schemaClient = new SchemaClient(i, schemaDownLatch, schemaBarrier);
      schemaClients.add(schemaClient);
    }
    List<Future<Boolean>> futures = new ArrayList<>();
    for (SchemaClient schemaClient : schemaClients) {
      Future<Boolean> future = schemaExecutorService.submit(schemaClient);
      futures.add(future);
    }
    startTime = System.nanoTime();
    schemaExecutorService.shutdown();
    try {
      // wait for all dataClients finish test
      schemaDownLatch.await();
      for (Future<Boolean> future : futures) {
        Boolean result = future.get();
        if (!result) {
          LOGGER.error("Registering schema failed!");
          return false;
        }
      }
      schemaClients.stream()
          .map(SchemaClient::getMeasurement)
          .forEach(baseModeMeasurement::mergeCreateSchemaFinishTime);
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all threads finish.", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOGGER.error("Exception occurred during getting result of tasks.", e);
      Thread.currentThread().interrupt();
    }
    LOGGER.info("Registering schema successful!");
    MetaDataSchema.clearSchemaClientDataSchema();
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
    double elapseTime = (System.nanoTime() - startTime) / NANO_TO_SECOND;
    if (elapseTime < 0) {
      LOGGER.error("elapseTime is negative: {}", elapseTime);
    }
    measurement.setElapseTime(elapseTime);
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

  private static Map<Operation, String> sqlMap = new ConcurrentHashMap<>();

  /**
   * Each type of query is recorded once.
   *
   * @param operation
   * @param sql
   */
  public static void logSqlIfNotCollect(Operation operation, String sql) {
    sqlMap.computeIfAbsent(operation, k -> sql);
  }

  /** Output all query statements */
  private void printSqlStatements() {
    StringBuilder stringBuilder = new StringBuilder("\n");
    stringBuilder
        .append(
            "----------------------------------------------------------------------------SQL Statements Example---------------------------------------------------------------------")
        .append('\n');
    stringBuilder
        .append(String.format(RESULT_ITEM, "Operation"))
        .append(String.format(LATENCY_ITEM, "SQL"))
        .append("\n");
    for (Map.Entry<Operation, String> entry : sqlMap.entrySet()) {
      stringBuilder
          .append(String.format(RESULT_ITEM, entry.getKey()))
          .append(String.format(LATENCY_ITEM, entry.getValue()))
          .append("\n");
    }
    stringBuilder.append(
        "------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    System.out.println(stringBuilder.toString());
  }
}
