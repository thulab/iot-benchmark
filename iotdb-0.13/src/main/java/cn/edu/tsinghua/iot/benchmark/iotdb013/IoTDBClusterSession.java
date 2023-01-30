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

package cn.edu.tsinghua.iot.benchmark.iotdb013;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.DeviceQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.VerificationQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class IoTDBClusterSession extends IoTDBSessionBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBClusterSession.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private SessionPool[] sessions;
  private int currSession;
  private static final int MAX_SESSION_CONNECTION_PER_CLIENT = 3;

  public IoTDBClusterSession(DBConfig dbConfig) {
    super(dbConfig);
    createSessions();
  }

  private void createSessions() {
    sessions = new SessionPool[dbConfig.getHOST().size()];
    for (int i = 0; i < sessions.length; i++) {
      sessions[i] =
          new SessionPool(
              dbConfig.getHOST().get(i),
              Integer.parseInt(dbConfig.getPORT().get(i)),
              dbConfig.getUSERNAME(),
              dbConfig.getPASSWORD(),
              MAX_SESSION_CONNECTION_PER_CLIENT,
              config.isENABLE_THRIFT_COMPRESSION(),
              true);
    }
  }

  @Override
  public void init() throws TsdbException {
    // do nothing
    this.service = Executors.newSingleThreadExecutor();
  }

  @Override
  public Status insertOneBatchByRecord(Batch batch) {
    String deviceId = getDevicePath(batch.getDeviceSchema());
    int failRecord = 0;
    List<String> sensors =
        batch.getDeviceSchema().getSensors().stream()
            .map(Sensor::getName)
            .collect(Collectors.toList());
    for (Record record : batch.getRecords()) {
      long timestamp = record.getTimestamp();
      List<TSDataType> dataTypes =
          constructDataTypes(
              batch.getDeviceSchema().getSensors(), record.getRecordDataValue().size());
      try {
        if (config.isTEMPLATE()) {
          sessions[currSession].insertAlignedRecord(
              deviceId, timestamp, sensors, dataTypes, record.getRecordDataValue());
        } else {
          sessions[currSession].insertRecord(
              deviceId, timestamp, sensors, dataTypes, record.getRecordDataValue());
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        LOGGER.error("insert record failed", e);
        failRecord++;
      }
    }
    currSession = (currSession + 1) % sessions.length;

    if (failRecord == 0) {
      return new Status(true);
    } else {
      Exception e = new Exception("failRecord number is " + failRecord);
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status insertOneBatchByRecords(Batch batch) {
    List<String> deviceIds = new ArrayList<>();
    String deviceId = getDevicePath(batch.getDeviceSchema());
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<String> sensors =
        batch.getDeviceSchema().getSensors().stream()
            .map(Sensor::getName)
            .collect(Collectors.toList());
    for (Record record : batch.getRecords()) {
      deviceIds.add(deviceId);
      times.add(record.getTimestamp());
      measurementsList.add(sensors);
      valuesList.add(record.getRecordDataValue());
      typesList.add(
          constructDataTypes(
              batch.getDeviceSchema().getSensors(), record.getRecordDataValue().size()));
    }

    future =
        service.submit(
            () -> {
              try {
                if (config.isVECTOR()) {
                  sessions[currSession].insertAlignedRecords(
                      deviceIds, times, measurementsList, typesList, valuesList);
                } else {
                  sessions[currSession].insertRecords(
                      deviceIds, times, measurementsList, typesList, valuesList);
                }
              } catch (IoTDBConnectionException | StatementExecutionException e) {
                LOGGER.error("insert records failed", e);
              }
            });

    Status status = waitFuture();
    currSession = (currSession + 1) % sessions.length;
    return status;
  }

  @Override
  public Status insertOneBatchByTablet(Batch batch) {
    Tablet tablet = genTablet(batch);

    future =
        service.submit(
            () -> {
              try {
                if (config.isVECTOR()) {
                  sessions[currSession].insertAlignedTablet(tablet);
                } else {
                  sessions[currSession].insertTablet(tablet);
                }
              } catch (IoTDBConnectionException | StatementExecutionException e) {
                LOGGER.error("insert tablet failed", e);
              }
            });

    Status status = waitFuture();
    currSession = (currSession + 1) % sessions.length;
    return status;
  }

  @Override
  protected Status executeQueryAndGetStatus(String sql, Operation operation) {
    String executeSQL;
    if (config.isIOTDB_USE_DEBUG() && random.nextDouble() < config.getIOTDB_USE_DEBUG_RATIO()) {
      executeSQL = "debug " + sql;
    } else {
      executeSQL = sql;
    }
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), executeSQL);
    }
    AtomicLong queryResultPointNum = new AtomicLong();
    AtomicBoolean isOk = new AtomicBoolean(true);

    try {
      List<List<Object>> records = new ArrayList<>();
      future =
          service.submit(
              () -> {
                long resultNum = 0;
                try {
                  SessionDataSetWrapper sessionDataSet =
                      sessions[currSession].executeQueryStatement(executeSQL);
                  while (sessionDataSet.hasNext()) {
                    RowRecord rowRecord = sessionDataSet.next();
                    switch (operation) {
                      case LATEST_POINT_QUERY:
                        resultNum++;
                        break;
                      default:
                        resultNum += rowRecord.getFields().size();
                        break;
                    }
                    if (config.isIS_COMPARISON()) {
                      List<Object> record = new ArrayList<>();
                      switch (operation) {
                        case AGG_RANGE_QUERY:
                        case AGG_VALUE_QUERY:
                        case AGG_RANGE_VALUE_QUERY:
                          break;
                        default:
                          record.add(rowRecord.getTimestamp());
                          break;
                      }
                      List<Field> fields = rowRecord.getFields();
                      for (int i = 0; i < fields.size(); i++) {
                        switch (operation) {
                          case LATEST_POINT_QUERY:
                            if (i == 0 || i == 2) {
                              continue;
                            }
                          default:
                            break;
                        }
                        record.add(fields.get(i).toString());
                      }
                      records.add(record);
                    }
                  }
                  sessionDataSet.close();
                } catch (StatementExecutionException | IoTDBConnectionException e) {
                  LOGGER.error("exception occurred when execute query={}", executeSQL, e);
                  isOk.set(false);
                }
                queryResultPointNum.set(resultNum);
              });
      try {
        future.get(config.getREAD_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        future.cancel(true);
        return new Status(false, queryResultPointNum.get(), e, executeSQL);
      }
      currSession = (currSession + 1) % sessions.length;
      if (isOk.get()) {
        if (config.isIS_COMPARISON()) {
          return new Status(true, queryResultPointNum.get(), executeSQL, records);
        } else {
          return new Status(true, queryResultPointNum.get());
        }
      } else {
        return new Status(
            false, queryResultPointNum.get(), new Exception("Failed to execute."), executeSQL);
      }
    } catch (Exception e) {
      return new Status(false, queryResultPointNum.get(), e, executeSQL);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum.get(), new Exception(t), executeSQL);
    }
  }

  /**
   * Using in verification
   *
   * @param verificationQuery
   */
  @Override
  public Status verificationQuery(VerificationQuery verificationQuery) {
    DeviceSchema deviceSchema = verificationQuery.getDeviceSchema();
    List<DeviceSchema> deviceSchemas = new ArrayList<>();
    deviceSchemas.add(deviceSchema);

    List<Record> records = verificationQuery.getRecords();
    if (records == null || records.size() == 0) {
      return new Status(
          false,
          new TsdbException("There are no records in verficationQuery."),
          "There are no records in verficationQuery.");
    }

    StringBuffer sql = new StringBuffer();
    sql.append(getSimpleQuerySqlHead(deviceSchemas));
    Map<Long, List<Object>> recordMap = new HashMap<>();
    sql.append(" WHERE time = ").append(records.get(0).getTimestamp());
    recordMap.put(records.get(0).getTimestamp(), records.get(0).getRecordDataValue());
    for (int i = 1; i < records.size(); i++) {
      Record record = records.get(i);
      sql.append(" or time = ").append(record.getTimestamp());
      recordMap.put(record.getTimestamp(), record.getRecordDataValue());
    }
    long point = 0;
    int line = 0;
    try {
      SessionDataSetWrapper sessionDataSet =
          sessions[currSession].executeQueryStatement(sql.toString());
      while (sessionDataSet.hasNext()) {
        RowRecord rowRecord = sessionDataSet.next();
        long timeStamp = rowRecord.getTimestamp();
        List<Object> values = recordMap.get(timeStamp);
        for (int i = 0; i < values.size(); i++) {
          String value = rowRecord.getFields().get(i).toString();
          String target = String.valueOf(values.get(i));
          if (!value.equals(target)) {
            LOGGER.error("Using SQL: " + sql + ",Expected:" + value + " but was: " + target);
          } else {
            point++;
          }
        }
        line++;
      }
      sessionDataSet.close();
      currSession = (currSession + 1) % sessions.length;
    } catch (Exception e) {
      LOGGER.error("Query Error: " + sql);
      return new Status(false, new TsdbException("Failed to query"), "Failed to query.");
    }
    if (recordMap.size() != line) {
      LOGGER.error(
          "Using SQL: " + sql + ",Expected line:" + recordMap.size() + " but was: " + line);
    }
    return new Status(true, point);
  }

  @Override
  public Status deviceQuery(DeviceQuery deviceQuery) throws SQLException, TsdbException {
    DeviceSchema deviceSchema = deviceQuery.getDeviceSchema();
    String sql =
        getDeviceQuerySql(
            deviceSchema, deviceQuery.getStartTimestamp(), deviceQuery.getEndTimestamp());
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("IoTDB:" + sql);
    }
    List<List<Object>> result = new ArrayList<>();
    try {
      SessionDataSetWrapper sessionDataSet = sessions[currSession].executeQueryStatement(sql);
      while (sessionDataSet.hasNext()) {
        List<Object> line = new ArrayList<>();
        RowRecord rowRecord = sessionDataSet.next();
        line.add(rowRecord.getTimestamp());
        List<Field> fields = rowRecord.getFields();
        for (int i = 0; i < fields.size(); i++) {
          line.add(fields.get(i).getStringValue());
        }
        result.add(line);
      }
      sessionDataSet.close();
      currSession = (currSession + 1) % sessions.length;
    } catch (Exception e) {
      LOGGER.error("Query Error: " + sql + " exception:" + e.getMessage());
      return new Status(false, new TsdbException("Failed to query"), "Failed to query.");
    }

    return new Status(true, 0, sql, result);
  }

  @Override
  public DeviceSummary deviceSummary(DeviceQuery deviceQuery) throws SQLException, TsdbException {
    DeviceSchema deviceSchema = deviceQuery.getDeviceSchema();
    int totalLineNumber = 0;
    long minTimeStamp = 0, maxTimeStamp = 0;
    try {
      SessionDataSetWrapper sessionDataSet =
          sessions[currSession].executeQueryStatement(getTotalLineNumberSql(deviceSchema));
      RowRecord rowRecord = sessionDataSet.next();
      totalLineNumber = Integer.parseInt(rowRecord.getFields().get(0).toString());
      sessionDataSet.close();

      sessionDataSet =
          sessions[currSession].executeQueryStatement(getMaxTimeStampSql(deviceSchema));
      rowRecord = sessionDataSet.next();
      maxTimeStamp = rowRecord.getTimestamp();
      sessionDataSet.close();

      sessionDataSet =
          sessions[currSession].executeQueryStatement(getMinTimeStampSql(deviceSchema));
      rowRecord = sessionDataSet.next();
      minTimeStamp = rowRecord.getTimestamp();
      sessionDataSet.close();
    } catch (IoTDBConnectionException e) {
      throw new TsdbException("Failed to connect to IoTDB:" + e.getMessage());
    } catch (StatementExecutionException e) {
      throw new TsdbException("Failed to execute statement:" + e.getMessage());
    }
    return new DeviceSummary(deviceSchema.getDevice(), totalLineNumber, minTimeStamp, maxTimeStamp);
  }

  private Status waitFuture() {
    try {
      future.get(config.getWRITE_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      future.cancel(true);
      return new Status(false, 0, e, e.toString());
    }

    return new Status(true);
  }

  @Override
  public void close() throws TsdbException {
    for (SessionPool sessionPool : sessions) {
      if (sessionPool != null) {
        sessionPool.close();
      }
    }
    if (ioTDBConnection != null) {
      ioTDBConnection.close();
    }
    this.service.shutdown();
  }
}
