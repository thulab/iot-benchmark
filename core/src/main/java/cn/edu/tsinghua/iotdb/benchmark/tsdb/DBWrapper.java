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

import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.TestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DBWrapper implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBWrapper.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  private static final double NANO_TO_SECOND = 1000000000.0d;
  private static final double NANO_TO_MILLIS = 1000000.0d;
  private static final String ERROR_LOG = "Failed to do {} because unexpected exception: ";

  private IDatabase db;
  private Measurement measurement;
  private TestDataPersistence recorder;

  /**
   * Use DBFactory to get database
   *
   * @param dbConfig
   * @param measurement
   */
  public DBWrapper(DBConfig dbConfig, Measurement measurement) {
    DBFactory dbFactory = new DBFactory();
    try {
      db = dbFactory.getDatabase(dbConfig);
    } catch (Exception e) {
      LOGGER.error("Failed to get database because", e);
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
      long start = System.nanoTime();
      status = db.insertOneBatch(batch);
      status = measureOneBatch(status, operation, batch, start);
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

  @Override
  public Status insertOneSensorBatch(Batch batch) throws DBConnectException {
    Status status = null;
    Operation operation = Operation.INGESTION;
    try {
      long start = System.nanoTime();
      status = db.insertOneSensorBatch(batch);
      status = measureOneBatch(status, operation, batch, start);
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

  /**
   * Measure one batch
   *
   * @param status
   * @param operation
   * @param batch
   * @param start
   * @return
   */
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
      long start = System.nanoTime();
      status = db.preciseQuery(preciseQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
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
      long start = System.nanoTime();
      status = db.rangeQuery(rangeQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
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
      long start = System.nanoTime();
      status = db.valueRangeQuery(valueRangeQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
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
      long start = System.nanoTime();
      status = db.aggRangeQuery(aggRangeQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
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
      long start = System.nanoTime();
      status = db.aggValueQuery(aggValueQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
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
      long start = System.nanoTime();
      status = db.aggRangeValueQuery(aggRangeValueQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
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
      long start = System.nanoTime();
      status = db.groupByQuery(groupByQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
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
      long start = System.nanoTime();
      status = db.latestPointQuery(latestPointQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
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
      long start = System.nanoTime();
      status = db.rangeQueryOrderByDesc(rangeQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
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
      long start = System.nanoTime();
      status = db.valueRangeQueryOrderByDesc(valueRangeQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  /**
   * Using in verification
   *
   * @param verificationQuery
   */
  @Override
  public Status verificationQuery(VerificationQuery verificationQuery) {
    Status status = null;
    Operation operation = Operation.VERIFICATION_QUERY;
    String device = verificationQuery.getDeviceSchema().getDevice();
    try {
      long start = System.nanoTime();
      status = db.verificationQuery(verificationQuery);
      long end = System.nanoTime();
      status.setTimeCost(end - start);
      handleQueryOperation(status, operation, device);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e, device);
    }
    return status;
  }

  @Override
  public void init() throws TsdbException {
    db.init();
  }

  @Override
  public void cleanup() throws TsdbException {
    // start cleanup database
    db.cleanup();
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
    db.close();
    if (recorder != null) {
      recorder.closeAsync();
    }
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    double createSchemaTimeInSecond;
    long end = 0;
    long start = 0;
    LOGGER.info("Registering schema...");
    try {
      if (config.isCREATE_SCHEMA()) {
        start = System.nanoTime();
        db.registerSchema(schemaList);
        end = System.nanoTime();
      }
      createSchemaTimeInSecond = (end - start) / NANO_TO_SECOND;
      measurement.setCreateSchemaTime(createSchemaTimeInSecond);
    } catch (Exception e) {
      measurement.setCreateSchemaTime(0);
      throw new TsdbException(e);
    }
  }

  /**
   * Measure ok operation 1. operation is execute as expected way 2. occurs expected exception
   *
   * @param status
   * @param operation
   * @param okPointNum
   * @param device
   */
  private void measureOkOperation(
      Status status, Operation operation, int okPointNum, String device) {
    double latencyInMillis = status.getTimeCost() / NANO_TO_MILLIS;
    if (latencyInMillis < 0) {
      LOGGER.warn(
          "Operation {} may have exception since the latency is negative, set it to zero",
          operation.getName());
      latencyInMillis = 0;
    }
    measurement.addOperationLatency(operation, latencyInMillis);
    measurement.addOkOperationNum(operation);
    measurement.addOkPointNum(operation, okPointNum);
    recorder.saveOperationResultAsync(
        operation.getName(), okPointNum, 0, latencyInMillis, "", device);
  }

  /**
   * Handle unexpected exception
   *
   * @param status
   * @param operation
   */
  private void handleQueryOperation(Status status, Operation operation, String device) {
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
      measurement.addFailOperationNum(operation);
      // currently we do not have expected result point number for query
      recorder.saveOperationResultAsync(
          operation.getName(), 0, 0, 0, status.getException().toString(), device);
    }
  }

  /**
   * Handle unexpected query exception
   *
   * @see DBWrapper
   * @param operation
   * @param e
   */
  private void handleUnexpectedQueryException(Operation operation, Exception e, String device) {
    measurement.addFailOperationNum(operation);
    // currently we do not have expected result point number for query
    LOGGER.error(ERROR_LOG, operation, e);
    recorder.saveOperationResultAsync(operation.getName(), 0, 0, 0, e.toString(), device);
  }
}
