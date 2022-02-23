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

package cn.edu.tsinghua.iotdb.benchmark.iotdb013;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;

import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.DeviceQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.VerificationQuery;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class IoTDBSession extends IoTDBSessionBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSession.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private final Session session;

  public IoTDBSession(DBConfig dbConfig) {
    super(dbConfig);
    session =
        new Session(
            dbConfig.getHOST().get(0),
            Integer.valueOf(dbConfig.getPORT().get(0)),
            dbConfig.getUSERNAME(),
            dbConfig.getPASSWORD(),
            true);
  }

  @Override
  public void init() throws TsdbException {
    try {
      if (config.isENABLE_THRIFT_COMPRESSION()) {
        session.open(true);
      } else {
        session.open();
      }
      this.service = Executors.newSingleThreadExecutor();
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Failed to add session", e);
    }
  }

  @Override
  public Status insertOneBatchByRecord(Batch batch) {
    String deviceId =
        ROOT_SERIES_NAME
            + "."
            + batch.getDeviceSchema().getGroup()
            + "."
            + batch.getDeviceSchema().getDevice();
    int failRecord = 0;
    List<String> sensors =
        batch.getDeviceSchema().getSensors().stream()
            .map(sensor -> sensor.getName())
            .collect(Collectors.toList());

    for (Record record : batch.getRecords()) {
      long timestamp = record.getTimestamp();
      List<TSDataType> dataTypes =
          constructDataTypes(
              batch.getDeviceSchema().getSensors(), record.getRecordDataValue().size());
      try {
        if (config.isTEMPLATE()) {
          session.insertAlignedRecord(
              deviceId + ".vector", timestamp, sensors, dataTypes, record.getRecordDataValue());
        } else {
          session.insertRecord(
              deviceId, timestamp, sensors, dataTypes, record.getRecordDataValue());
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        failRecord++;
      }
    }
    if (failRecord == 0) {
      return new Status(true);
    } else {
      Exception e = new Exception("failRecord number is " + failRecord);
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status insertOneBatchByRecords(Batch batch) {
    String deviceId =
        ROOT_SERIES_NAME
            + "."
            + batch.getDeviceSchema().getGroup()
            + "."
            + batch.getDeviceSchema().getDevice();
    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<String> sensors =
        batch.getDeviceSchema().getSensors().stream()
            .map(sensor -> sensor.getName())
            .collect(Collectors.toList());

    for (Record record : batch.getRecords()) {
      if (config.isTEMPLATE()) deviceIds.add(deviceId + ".vector");
      else deviceIds.add(deviceId);
      times.add(record.getTimestamp());
      measurementsList.add(sensors);
      valuesList.add(record.getRecordDataValue());
      typesList.add(
          constructDataTypes(
              batch.getDeviceSchema().getSensors(), record.getRecordDataValue().size()));
    }
    try {
      if (config.isVECTOR()) {
        session.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
      } else {
        session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      }
      return new Status(true);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status insertOneBatchByTablet(Batch batch) {
    Tablet tablet = genTablet(batch);
    try {
      if (config.isVECTOR()) {
        session.insertAlignedTablet(tablet);
      } else {
        session.insertTablet(tablet);
      }
      return new Status(true);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  protected Status executeQueryAndGetStatus(String sql, Operation operation) {
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }
    AtomicInteger line = new AtomicInteger();
    AtomicInteger queryResultPointNum = new AtomicInteger();
    AtomicBoolean isOk = new AtomicBoolean(true);

    try {
      List<List<Object>> records = new ArrayList<>();
      future =
          service.submit(
              () -> {
                try {
                  SessionDataSet sessionDataSet = session.executeQueryStatement(sql);
                  while (sessionDataSet.hasNext()) {
                    RowRecord rowRecord = sessionDataSet.next();
                    line.getAndIncrement();
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
                  sessionDataSet.closeOperationHandle();
                } catch (StatementExecutionException | IoTDBConnectionException e) {
                  LOGGER.error("exception occurred when execute query={}", sql, e);
                  isOk.set(false);
                }
                queryResultPointNum.set(
                    line.get() * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM());
              });
      try {
        future.get(config.getREAD_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        future.cancel(true);
        return new Status(false, queryResultPointNum.get(), e, sql);
      }
      if (isOk.get()) {
        if (config.isIS_COMPARISON()) {
          return new Status(true, queryResultPointNum.get(), sql, records);
        } else {
          return new Status(true, queryResultPointNum.get());
        }
      } else {
        return new Status(
            false, queryResultPointNum.get(), new Exception("Failed to execute."), sql);
      }
    } catch (Exception e) {
      return new Status(false, queryResultPointNum.get(), e, sql);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum.get(), new Exception(t), sql);
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
    int point = 0;
    int line = 0;
    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement(sql.toString());
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
      SessionDataSet sessionDataSet = session.executeQueryStatement(sql);
      while (sessionDataSet.hasNext()) {
        List<Object> line = new ArrayList<>();
        RowRecord rowRecord = sessionDataSet.next();
        List<Field> fields = rowRecord.getFields();
        line.add(rowRecord.getTimestamp());
        for (int i = 0; i < fields.size(); i++) {
          line.add(fields.get(i).getStringValue());
        }
        result.add(line);
      }
      sessionDataSet.closeOperationHandle();
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
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(getTotalLineNumberSql(deviceSchema));
      RowRecord rowRecord = sessionDataSet.next();
      totalLineNumber = Integer.parseInt(rowRecord.getFields().get(0).toString());
      sessionDataSet.closeOperationHandle();

      sessionDataSet = session.executeQueryStatement(getMaxTimeStampSql(deviceSchema));
      rowRecord = sessionDataSet.next();
      maxTimeStamp = rowRecord.getTimestamp();
      sessionDataSet.closeOperationHandle();

      sessionDataSet = session.executeQueryStatement(getMinTimeStampSql(deviceSchema));
      rowRecord = sessionDataSet.next();
      minTimeStamp = rowRecord.getTimestamp();
      sessionDataSet.closeOperationHandle();
    } catch (IoTDBConnectionException e) {
      throw new TsdbException("Failed to connect to IoTDB:" + e.getMessage());
    } catch (StatementExecutionException e) {
      throw new TsdbException("Failed to execute statement:" + e.getMessage());
    }
    return new DeviceSummary(deviceSchema.getDevice(), totalLineNumber, minTimeStamp, maxTimeStamp);
  }

  @Override
  public void cleanup() {
    try {
      session.executeNonQueryStatement(DELETE_SERIES_SQL);
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Failed to connect to IoTDB:" + e.getMessage());
    } catch (StatementExecutionException e) {
      LOGGER.error("Failed to execute statement:" + e.getMessage());
    }
  }

  @Override
  public void close() throws TsdbException {
    try {
      if (session != null) {
        session.close();
      }
      if (ioTDBConnection != null) {
        ioTDBConnection.close();
      }
      this.service.shutdown();
    } catch (IoTDBConnectionException ioTDBConnectionException) {
      LOGGER.error("Failed to close session.");
      throw new TsdbException(ioTDBConnectionException);
    }
  }
}
