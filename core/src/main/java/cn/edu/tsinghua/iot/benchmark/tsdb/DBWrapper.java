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

package cn.edu.tsinghua.iot.benchmark.tsdb;

import cn.edu.tsinghua.iot.benchmark.client.generate.RecordComparator;
import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.TestDataPersistence;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.DeviceQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.Query;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.VerificationQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DBWrapper implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBWrapper.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  private static final double NANO_TO_SECOND = 1000000000.0d;
  private static final double NANO_TO_MILLIS = 1000000.0d;
  private static final String ERROR_LOG = "Failed to do {} because unexpected exception: ";

  private List<IDatabase> databases = new ArrayList<>();
  private final Measurement measurement = new Measurement();
  private TestDataPersistence recorder;

  /** Use DBFactory to get database */
  public DBWrapper(List<DBConfig> dbConfigs) {
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
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    recorder = persistenceFactory.getPersistence();
  }

  public Measurement getMeasurement() {
    return measurement;
  }

  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    Status status = null;
    Operation operation = Operation.INGESTION;
    try {
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.insertOneBatchWithCheck(batch);
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
  private Status measureOneBatch(Status status, Operation operation, IBatch batch, long start) {
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
      doComparisonByRecord(preciseQuery, operation, statuses);
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
      doComparisonByRecord(rangeQuery, operation, statuses);
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
      doComparisonByRecord(valueRangeQuery, operation, statuses);
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
      doComparisonByRecord(aggRangeQuery, operation, statuses);
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
      doComparisonByRecord(aggValueQuery, operation, statuses);
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
      doComparisonByRecord(aggRangeValueQuery, operation, statuses);
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
      doComparisonByRecord(groupByQuery, operation, statuses);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public Status groupByQueryOrderByDesc(GroupByQuery groupByQuery) {
    Status status = null;
    Operation operation = Operation.GROUP_BY_QUERY_ORDER_BY_TIME_DESC;
    String device = "No Device";
    if (groupByQuery.getDeviceSchema().size() > 0) {
      device = groupByQuery.getDeviceSchema().get(0).getDevice();
    }
    try {
      List<Status> statuses = new ArrayList<>();
      for (IDatabase database : databases) {
        long start = System.nanoTime();
        status = database.groupByQueryOrderByDesc(groupByQuery);
        long end = System.nanoTime();
        status.setTimeCost(end - start);
        handleQueryOperation(status, operation, device);
        statuses.add(status);
      }
      doComparisonByRecord(groupByQuery, operation, statuses);
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
      doComparisonByRecord(latestPointQuery, operation, statuses);
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
      doComparisonByRecord(rangeQuery, operation, statuses);
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
      doComparisonByRecord(valueRangeQuery, operation, statuses);
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
      doComparisonByRecord(verificationQuery, operation, statuses);
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
        statuses.add(status);
      }
      doPointComparison(statuses, deviceQuery);
      for (Status sta : statuses) {
        handleQueryOperation(sta, operation, device);
      }
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public DeviceSummary deviceSummary(DeviceQuery deviceQuery) throws SQLException, TsdbException {
    DeviceSummary deviceSummary = null;
    try {
      List<DeviceSummary> deviceSummaries = new ArrayList<>();
      for (IDatabase database : databases) {
        deviceSummary = database.deviceSummary(deviceQuery);
        if (deviceSummary == null) {
          LOGGER.error("Failed to get summary: {}", database.getClass().getName());
          continue;
        }
        deviceSummaries.add(deviceSummary);
      }
      DeviceSummary base = deviceSummaries.get(0);
      for (int i = 1; i < deviceSummaries.size(); i++) {
        if (!base.equals(deviceSummaries.get(i))) {
          LOGGER.error("Error number of different database: ");
          LOGGER.error("DB1:" + base);
          LOGGER.error("DB2:" + deviceSummaries.get(i));
          return null;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to get summary of device");
      e.printStackTrace();
      return null;
    }
    LOGGER.info("Device Summary:" + deviceSummary.toString());
    return deviceSummary;
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
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    double createSchemaTimeInSecond = 0.0;
    LOGGER.info("Registering schema...");
    try {
      for (IDatabase database : databases) {
        Double registerTime = database.registerSchema(schemaList);
        if (null == registerTime) {
          LOGGER.error("Failed to create schema for {}.", database.getClass().getName());
          return null;
        }
        createSchemaTimeInSecond = Math.max(createSchemaTimeInSecond, registerTime);
      }
      measurement.setCreateSchemaFinishTime(createSchemaTimeInSecond);
    } catch (Exception e) {
      measurement.setCreateSchemaFinishTime(0.0);
      throw new TsdbException(e);
    }
    return createSchemaTimeInSecond;
  }

  /** Measure ok operation 1. operation is execute as expected way 2. occurs expected exception */
  private void measureOkOperation(
      Status status, Operation operation, long okPointNum, String device) {
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

  private int doPointComparison(List<Status> statuses, DeviceQuery deviceQuery) {
    int totalPointNumber = 0;

    long start = System.nanoTime();
    Status status1 = statuses.get(0);
    Status status2 = statuses.get(1);
    List<List<Object>> records1 = status1.getRecords();
    List<List<Object>> records2 = status2.getRecords();
    int lines1 = records1.size();
    int lines2 = records2.size();
    if (lines1 != lines2) {
      LOGGER.error("Line number different. DeviceQuery:" + deviceQuery.getQueryAttrs());
      return -1;
    }
    for (int i = 0; i < lines1; i++) {
      List<Object> record1 = records1.get(i);
      List<Object> record2 = records2.get(i);
      StringBuilder stringBuilder1 = new StringBuilder(record1.get(0).toString());
      StringBuilder stringBuilder2 = new StringBuilder(record2.get(0).toString());
      // compare
      if (record1.size() != record2.size()) {
        LOGGER.error("Column number different. DeviceQuery:" + deviceQuery.getQueryAttrs());
        return -1;
      }
      for (int j = 0; j < record1.size(); j++) {
        stringBuilder1.append(",").append(record1.get(j));
        stringBuilder2.append(",").append(record2.get(j));
        if (j != 0) {
          totalPointNumber++;
        }
      }
      if (!stringBuilder1.toString().equals(stringBuilder2.toString())) {
        LOGGER.error("DeviceQuery:" + deviceQuery.getQueryAttrs());
        LOGGER.error("In DB1 line: " + stringBuilder1);
        LOGGER.error("In DB2 line: " + stringBuilder2);
        return -1;
      }
    }
    long end = System.nanoTime();
    status1.setTimeCost(end - start + status1.getTimeCost());
    status2.setTimeCost(end - start + status2.getTimeCost());
    status1.setQueryResultPointNum(totalPointNumber);
    status2.setQueryResultPointNum(totalPointNumber);
    return lines1;
  }

  private boolean doComparisonByRecord(Query query, Operation operation, List<Status> statuses) {
    if (config.isIS_COMPARISON() && statuses.size() >= 2) {
      Status status1 = statuses.get(0);
      Status status2 = statuses.get(1);
      boolean isError = false;
      if (status1 != null
          && status2 != null
          && status1.getRecords() != null
          && status2.getRecords() != null) {
        long point1 = status1.getQueryResultPointNum();
        long point2 = status2.getQueryResultPointNum();
        if (!hasDifference(operation) && point1 != point2) {
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
            records1.sort(new RecordComparator());
            records2.sort(new RecordComparator());
          }
          // 顺序比较
          int i = 0, j = 0;
          for (; i < point1 && j < point2; i++, j++) {
            String firstLine =
                records1.get(i).stream().map(String::valueOf).collect(Collectors.joining(","));
            String secondLine =
                records2.get(j).stream().map(String::valueOf).collect(Collectors.joining(","));
            if (!firstLine.equals(secondLine)) {
              if (hasDifference(operation)) {
                int index = records1.get(i).size() - 1;
                String value = String.valueOf(records1.get(i).get(index));
                if (null != value && 0 != Integer.parseInt(value)) {
                  isError = true;
                  break;
                } else {
                  j--;
                }
              } else {
                isError = true;
                break;
              }
            }
          }
          if (j != point2) {
            isError = true;
          }
          if (i != point1) {
            if (hasDifference(operation)) {
              for (; i < point1; i++) {
                int index = records1.get(i).size() - 1;
                String value = String.valueOf(records1.get(i).get(index));
                if (null != value && 0 != Integer.parseInt(value)) {
                  isError = true;
                  break;
                }
              }
            } else {
              isError = true;
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

  private boolean hasDifference(Operation operation) {
    switch (operation) {
      case GROUP_BY_QUERY:
      case AGG_RANGE_QUERY:
      case AGG_RANGE_VALUE_QUERY:
      case AGG_VALUE_QUERY:
        return true;
      default:
        return false;
    }
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
