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

package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.client.generate.RecordComparator;
import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.TestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DBWrapper implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBWrapper.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  private static final double NANO_TO_SECOND = 1000000000.0d;
  private static final double NANO_TO_MILLIS = 1000000.0d;
  private static final String ERROR_LOG = "Failed to do {} because unexpected exception: ";

  private int lineNumber = 0;

  private List<IDatabase> databases = new ArrayList<>();
  private Measurement measurement;
  private TestDataPersistence recorder;

  /** Use DBFactory to get database */
  public DBWrapper(List<DBConfig> dbConfigs, Measurement measurement) {
    DBFactory dbFactory = new DBFactory();
    for (DBConfig dbConfig : dbConfigs) {
      try {
        IDatabase database = dbFactory.getDatabase(dbConfig);
        if (database == null) {
          LOGGER.error("Failed to get database: " + dbConfig);
        }
        databases.add(database);
      } catch (Exception e) {
        LOGGER.error("Failed to get database because", e);
      }
    }
    this.measurement = measurement;
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    recorder = persistenceFactory.getPersistence();
  }

  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    Status status = null;
    Operation operation = Operation.INGESTION;
    try {
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.insertOneBatch(batch);
        status = measureOneBatch(status, operation, batch, start);
      }
    } catch (DBConnectException ex) {
      throw ex;
    } catch (Exception e) {
      measurement.addFailOperationNum(operation);
      measurement.addFailPointNum(operation, batch.pointNum());
      recorder.saveOperationResultAsync(
          operation.getName(),
          0,
          batch.pointNum(),
          0,
          e.toString(),
          batch.getDeviceSchema().getDevice());
      LOGGER.error("Failed to insert one batch because unexpected exception: ", e);
    }
    return status;
  }

  /** Measure one batch */
  private Status measureOneBatch(Status status, Operation operation, Batch batch, long start) {
    long end = System.nanoTime();
    status.setTimeCost(end - start);
    if (status.isOk()) {
      measureOkOperation(status, operation, batch.pointNum(), batch.getDeviceSchema().getDevice());
      if (!config.isIS_QUIET_MODE()) {
        double timeInMillis = status.getTimeCost() / NANO_TO_MILLIS;
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        double throughput = batch.pointNum() * 1000 / timeInMillis;
        LOGGER.info(
            "{} insert one batch latency (device: {}, sg: {}) ,{}, ms, throughput ,{}, points/s",
            Thread.currentThread().getName(),
            batch.getDeviceSchema().getDevice(),
            batch.getDeviceSchema().getGroup(),
            formatTimeInMillis,
            throughput);
      }
    } else {
      measurement.addFailOperationNum(operation);
      measurement.addFailPointNum(operation, batch.pointNum());
      recorder.saveOperationResultAsync(
          operation.getName(),
          0,
          batch.pointNum(),
          0,
          status.getException().toString(),
          batch.getDeviceSchema().getDevice());
      LOGGER.error("Insert batch failed because", status.getException());
    }
    return status;
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    Status status = null;
    Operation operation = Operation.PRECISE_QUERY;
    String device = "No Device";
    if (preciseQuery.getDeviceSchema().size() > 0) {
      device = preciseQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.preciseQuery(preciseQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(preciseQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    Status status = null;
    Operation operation = Operation.RANGE_QUERY;
    String device = "No Device";
    if (rangeQuery.getDeviceSchema().size() > 0) {
      device = rangeQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.rangeQuery(rangeQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(rangeQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    Status status = null;
    Operation operation = Operation.VALUE_RANGE_QUERY;
    String device = "No Device";
    if (valueRangeQuery.getDeviceSchema().size() > 0) {
      device = valueRangeQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.valueRangeQuery(valueRangeQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(valueRangeQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    Status status = null;
    Operation operation = Operation.AGG_RANGE_QUERY;
    String device = "No Device";
    if (aggRangeQuery.getDeviceSchema().size() > 0) {
      device = aggRangeQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.aggRangeQuery(aggRangeQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(aggRangeQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    Status status = null;
    Operation operation = Operation.AGG_VALUE_QUERY;
    String device = "No Device";
    if (aggValueQuery.getDeviceSchema().size() > 0) {
      device = aggValueQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.aggValueQuery(aggValueQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(aggValueQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    Status status = null;
    Operation operation = Operation.AGG_RANGE_VALUE_QUERY;
    String device = "No Device";
    if (aggRangeValueQuery.getDeviceSchema().size() > 0) {
      device = aggRangeValueQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.aggRangeValueQuery(aggRangeValueQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        statuses.add(status);
      }
      handleQueryOperation(status, operation, device);
      doComparisonByRecord(aggRangeValueQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    Status status = null;
    Operation operation = Operation.GROUP_BY_QUERY;
    String device = "No Device";
    if (groupByQuery.getDeviceSchema().size() > 0) {
      device = groupByQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.groupByQuery(groupByQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(groupByQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    Status status = null;
    Operation operation = Operation.LATEST_POINT_QUERY;
    String device = "No Device";
    if (latestPointQuery.getDeviceSchema().size() > 0) {
      device = latestPointQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.latestPointQuery(latestPointQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(latestPointQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    Status status = null;
    Operation operation = Operation.RANGE_QUERY_ORDER_BY_TIME_DESC;
    String device = "No Device";
    if (rangeQuery.getDeviceSchema().size() > 0) {
      device = rangeQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      rangeQuery.setDesc(true);
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.rangeQueryOrderByDesc(rangeQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(rangeQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    Status status = null;
    Operation operation = Operation.VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC;
    String device = "No Device";
    if (valueRangeQuery.getDeviceSchema().size() > 0) {
      device = valueRangeQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      valueRangeQuery.setDesc(true);
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.valueRangeQueryOrderByDesc(valueRangeQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(valueRangeQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  /** Using in verification */
  @Override
  public Status verificationQuery(VerificationQuery verificationQuery) {
    Status status = null;
    Operation operation = Operation.VERIFICATION_QUERY;
    String device = verificationQuery.getDeviceSchema().getDevice();
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.verificationQuery(verificationQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(verificationQuery, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status deviceQuery(DeviceQuery deviceQuery) throws SQLException {
    Status status = null;
    Operation operation = Operation.DEVICE_QUERY;
    String device = deviceQuery.getDeviceSchema().getDevice();
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.deviceQuery(deviceQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        LOGGER.info(
            database.getClass().getName()
                + " use "
                + (end - start)
                + " ms to query "
                + deviceQuery.getDeviceSchema().getDevice());
        statuses.add(status);
      }
      LOGGER.info("Start Compare:" + deviceQuery.getDeviceSchema().getDevice());
      doPointComparison(statuses, deviceQuery);
      LOGGER.info("Finish Compare:" + deviceQuery.getDeviceSchema().getDevice());
      for (Status sta : statuses) {
        handleQueryOperation(sta, operation, device);
      }
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public void init() throws TsdbException {
    for (IDatabase database : databases) {
      database.init();
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    // start cleanup database
    for (IDatabase database : databases) {
      database.cleanup();
    }
    // waiting for deletion of database
    try {
      LOGGER.info("Waiting {}ms for old data deletion.", config.getINIT_WAIT_TIME());
      Thread.sleep(config.getINIT_WAIT_TIME());
    } catch (InterruptedException e) {
      LOGGER.warn("Failed to wait {}ms for old data deletion.", config.getINIT_WAIT_TIME());
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() throws TsdbException {
    for (IDatabase database : databases) {
      database.close();
    }
    if (recorder != null) {
      recorder.closeAsync();
    }
  }

  @Override
  public boolean registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    double createSchemaTimeInSecond;
    long end = 0;
    long start = 0;
    LOGGER.info("Registering schema...");
    try {
      start = System.nanoTime();
      if (config.isCREATE_SCHEMA()) {
        for (IDatabase database : databases) {
          if (!database.registerSchema(schemaList)) {
            LOGGER.error("Failed to create schema for {}.", database.getClass().getName());
            return false;
          }
        }
      }
      end = System.nanoTime();
      createSchemaTimeInSecond = (end - start) / NANO_TO_SECOND;
      measurement.setCreateSchemaTime(createSchemaTimeInSecond);
    } catch (Exception e) {
      measurement.setCreateSchemaTime(0);
      throw new TsdbException(e);
    }
    return true;
  }

  /** Measure ok operation 1. operation is execute as expected way 2. occurs expected exception */
  private void measureOkOperation(
      Status status, Operation operation, int okPointNum, String device) {
    double latencyInMillis = status.getTimeCost() / NANO_TO_MILLIS;
    if (config.isUSE_MEASUREMENT()) {
      if (latencyInMillis < 0) {
        LOGGER.warn(
            "Operation {} may have exception since the latency is negative, set it to zero",
            operation.getName());
        latencyInMillis = 0;
      }
      measurement.addOperationLatency(operation, latencyInMillis);
      measurement.addOkOperationNum(operation);
      measurement.addOkPointNum(operation, okPointNum);
    }
    recorder.saveOperationResultAsync(
        operation.getName(), okPointNum, 0, latencyInMillis, "", device);
  }

  private boolean doPointComparison(List<Status> statuses, DeviceQuery deviceQuery) {
    ScheduledExecutorService pointService = Executors.newSingleThreadScheduledExecutor();

    int totalPointNumber = 0;

    String currentThread = Thread.currentThread().getName();
    // print current progress periodically
    pointService.scheduleAtFixedRate(
        () -> {
          String percent =
              String.format(
                  "%.2f",
                  (lineNumber + 1)
                      * 100.0D
                      / (config.getLOOP() * config.getBATCH_SIZE_PER_WRITE()));
          LOGGER.info(
              "{} Loop {} ({}%) syntheticClient for {} is done.",
              currentThread, (lineNumber + 1), percent, deviceQuery.getDeviceSchema().getDevice());
        },
        1,
        config.getLOG_PRINT_INTERVAL(),
        TimeUnit.SECONDS);
    try {
      long start = System.nanoTime();
      Status status1 = statuses.get(0);
      Status status2 = statuses.get(1);
      ResultSet resultSet1 = status1.getResultSet();
      ResultSet resultSet2 = status2.getResultSet();
      int col1 = resultSet1.getMetaData().getColumnCount();
      int col2 = resultSet2.getMetaData().getColumnCount();
      if (col1 != col2) {
        LOGGER.error("DeviceQuery:" + deviceQuery.getQueryAttrs());
        resultSet1.close();
        resultSet2.close();
        return false;
      }
      resultSet1.next();
      resultSet2.next();
      while (true) {
        StringBuilder stringBuilder1 = new StringBuilder(resultSet1.getObject(1).toString());
        StringBuilder stringBuilder2 = new StringBuilder(resultSet2.getObject(1).toString());
        // compare
        for (int j = 2; j <= resultSet1.getMetaData().getColumnCount(); j++) {
          stringBuilder1.append(",").append(resultSet1.getObject(j));
          stringBuilder2.append(",").append(resultSet1.getObject(j));
          totalPointNumber++;
        }
        if (!stringBuilder1.toString().equals(stringBuilder2.toString())) {
          LOGGER.error("DeviceQuery:" + deviceQuery.getQueryAttrs());
          LOGGER.error("In DB1 line: " + stringBuilder1);
          LOGGER.error("In DB2 line: " + stringBuilder2);
          resultSet1.close();
          resultSet2.close();
          return false;
        }
        boolean b1 = resultSet1.next();
        boolean b2 = resultSet2.next();
        if (!b1 | !b2) {
          if (!b1 & !b2) {
            break;
          }
          LOGGER.error("DeviceQuery(Different Length):" + deviceQuery.getQueryAttrs());
          resultSet1.close();
          resultSet2.close();
          return false;
        }
        lineNumber++;
      }
      long end = System.nanoTime();
      status1.setTimeCost(end - start + status1.getTimeCost());
      status2.setTimeCost(end - start + status2.getTimeCost());
      status1.setQueryResultPointNum(totalPointNumber);
      status2.setQueryResultPointNum(totalPointNumber);
      LOGGER.info(
          "Finish Device: "
              + deviceQuery.getDeviceSchema().getDevice()
              + " with "
              + (lineNumber + 1)
              + " line.");
      lineNumber = 0;
    } catch (SQLException e) {
      LOGGER.error("Failed to do DEVICE_QUERY because ", e);
      return false;
    }
    pointService.shutdown();
    return true;
  }

  private boolean doComparisonByRecord(Query query, List<Status> statuses) {
    if (config.isIS_COMPARISON() && statuses.size() >= 2) {
      Status status1 = statuses.get(0);
      Status status2 = statuses.get(1);
      boolean isError = false;
      if (status1 != null
          && status2 != null
          && status1.getRecords() != null
          && status2.getRecords() != null) {
        int point1 = status1.getQueryResultPointNum();
        int point2 = status2.getQueryResultPointNum();
        if (point1 != point2) {
          isError = true;
        } else if (point1 != 0) {
          List<List<Object>> records1 = status1.getRecords();
          List<List<Object>> records2 = status2.getRecords();
          boolean needSort = true;
          if (query instanceof RangeQuery) {
            if (((RangeQuery) query).isDesc()) {
              needSort = false;
            }
          }
          if (needSort) {
            Collections.sort(records1, new RecordComparator());
            Collections.sort(records2, new RecordComparator());
          }
          // 顺序比较
          for (int i = 0; i < point1; i++) {
            String firstLine =
                String.join(
                    ",",
                    records1.get(i).stream().map(String::valueOf).collect(Collectors.toList()));
            String secondLine =
                String.join(
                    ",",
                    records2.get(i).stream().map(String::valueOf).collect(Collectors.toList()));
            if (!firstLine.equals(secondLine)) {
              isError = true;
              break;
            }
          }
        }
      }
      if (isError) {
        doErrorLog(query.getClass().getSimpleName(), status1, status2);
      }
    }
    return true;
  }

  private void doErrorLog(String queryName, Status status1, Status status2) {
    LOGGER.error(
        queryName
            + " DB1 point: "
            + status1.getQueryResultPointNum()
            + " and DB2 point: "
            + status2.getQueryResultPointNum()
            + "\n"
            + config.getDbConfig().getDB_SWITCH()
            + ":"
            + status1.getSql()
            + "\n"
            + config.getANOTHER_DBConfig().getDB_SWITCH()
            + ":"
            + status2.getSql());
  }

  /** Handle unexpected exception */
  public void handleQueryOperation(Status status, Operation operation, String device) {
    if (status.isOk()) {
      measureOkOperation(status, operation, status.getQueryResultPointNum(), device);
      if (!config.isIS_QUIET_MODE()) {
        double timeInMillis = status.getTimeCost() / NANO_TO_MILLIS;
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        String currentThread = Thread.currentThread().getName();
        LOGGER.info(
            "{} complete {} with latency ,{}, ms ,{}, result points",
            currentThread,
            operation,
            formatTimeInMillis,
            status.getQueryResultPointNum());
      }
    } else {
      LOGGER.error("Execution fail: {}", status.getErrorMessage(), status.getException());
      if (config.isUSE_MEASUREMENT()) {
        measurement.addFailOperationNum(operation);
      }
      // currently, we do not have expected result point number for query
      recorder.saveOperationResultAsync(
          operation.getName(), 0, 0, 0, status.getException().toString(), device);
    }
  }

  /**
   * Handle unexpected query exception
   *
   * @see DBWrapper
   */
  public void handleUnexpectedQueryException(Operation operation, Exception e, String device) {
    if (config.isUSE_MEASUREMENT()) {
      measurement.addFailOperationNum(operation);
      // currently, we do not have expected result point number for query
      LOGGER.error(ERROR_LOG, operation, e);
      recorder.saveOperationResultAsync(operation.getName(), 0, 0, 0, e.toString(), device);
    }
  }
}
