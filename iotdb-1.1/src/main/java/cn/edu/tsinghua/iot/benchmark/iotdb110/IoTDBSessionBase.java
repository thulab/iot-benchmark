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

package cn.edu.tsinghua.iot.benchmark.iotdb110;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.OperationFailException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBInsertMode;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.DeviceQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.VerificationQuery;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class IoTDBSessionBase extends IoTDB {
  static Logger LOGGER;

  static final Config config = ConfigDescriptor.getInstance().getConfig();
  IBenchmarkSession sessionWrapper;

  private static final Map<String, Binary> binaryCache =
      new ConcurrentHashMap<>(config.getWORKLOAD_BUFFER_SIZE());

  public IoTDBSessionBase(DBConfig dbConfig) {
    super(dbConfig);
  }

  public Status insertOneBatchByTablet(IBatch batch) {
    Tablet tablet = genTablet(batch);
    task =
        service.submit(
            () -> {
              try {
                if (config.isVECTOR()) {
                  sessionWrapper.insertAlignedTablet(tablet);
                } else {
                  sessionWrapper.insertTablet(tablet);
                }
              } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new OperationFailException(e);
              }
            });
    return waitWriteTaskToFinishAndGetStatus();
  }

  public Status insertOneBatchByRecord(IBatch batch) {
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
        if (config.isVECTOR()) {
          sessionWrapper.insertAlignedRecord(
              deviceId, timestamp, sensors, dataTypes, record.getRecordDataValue());
        } else {
          sessionWrapper.insertRecord(
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

  public Status insertOneBatchByRecords(IBatch batch) {
    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<String> sensors =
        batch.getDeviceSchema().getSensors().stream()
            .map(Sensor::getName)
            .collect(Collectors.toList());
    while (true) {
      String deviceId = getDevicePath(batch.getDeviceSchema());
      for (Record record : batch.getRecords()) {
        deviceIds.add(deviceId);
        times.add(record.getTimestamp());
        measurementsList.add(sensors);
        valuesList.add(record.getRecordDataValue());
        typesList.add(
            constructDataTypes(
                batch.getDeviceSchema().getSensors(), record.getRecordDataValue().size()));
      }
      if (!batch.hasNext()) {
        break;
      }
      batch.next();
    }
    task =
        service.submit(
            () -> {
              try {
                if (config.isVECTOR()) {
                  sessionWrapper.insertAlignedRecords(
                      deviceIds, times, measurementsList, typesList, valuesList);
                } else {
                  sessionWrapper.insertRecords(
                      deviceIds, times, measurementsList, typesList, valuesList);
                }
              } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new OperationFailException(e);
              }
            });
    return waitWriteTaskToFinishAndGetStatus();
  }

  @Override
  protected Status addTailClausesAndExecuteQueryAndGetStatus(String sql, Operation operation) {
    if (config.getRESULT_ROW_LIMIT() >= 0) {
      sql += " limit " + config.getRESULT_ROW_LIMIT();
    }
    if (config.isALIGN_BY_DEVICE()) {
      sql += " align by device";
    }
    String executeSQL;
    if (config.isIOTDB_USE_DEBUG() && random.nextDouble() < config.getIOTDB_USE_DEBUG_RATIO()) {
      executeSQL = "debug " + sql;
    } else {
      executeSQL = sql;
    }
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), executeSQL);
    }
    AtomicInteger line = new AtomicInteger();
    AtomicLong queryResultPointNum = new AtomicLong();
    AtomicBoolean isOk = new AtomicBoolean(true);
    try {
      List<List<Object>> records = new ArrayList<>();
      task =
          service.submit(
              () -> {
                try {
                  ISessionDataSet sessionDataSet = sessionWrapper.executeQueryStatement(executeSQL);
                  if (config.isIS_COMPARISON()) {
                    while (sessionDataSet.hasNext()) {
                      RowRecord rowRecord = sessionDataSet.next();
                      line.getAndIncrement();
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
                  } else {
                    SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
                    while (iterator.next()) {
                      line.getAndIncrement();
                    }
                  }

                  sessionDataSet.close();
                } catch (StatementExecutionException | IoTDBConnectionException e) {
                  LOGGER.error("exception occurred when execute query={}", executeSQL, e);
                  isOk.set(false);
                }
                long resultPointNum = line.get();
                if (!Operation.LATEST_POINT_QUERY.equals(operation)
                    || !config.isALIGN_BY_DEVICE()) {
                  resultPointNum *= config.getQUERY_SENSOR_NUM();
                  resultPointNum *= config.getQUERY_DEVICE_NUM();
                }
                queryResultPointNum.set(resultPointNum);
              });
      try {
        task.get(config.getREAD_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        task.cancel(true);
        return new Status(false, queryResultPointNum.get(), e, executeSQL);
      }
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
   * @param verificationQuery the query of verification
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
          new TsdbException("There are no records in verificationQuery."),
          "There are no records in verificationQuery.");
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
      ISessionDataSet sessionDataSet = sessionWrapper.executeQueryStatement(sql.toString());
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
  public Status deviceQuery(DeviceQuery deviceQuery) {
    DeviceSchema deviceSchema = deviceQuery.getDeviceSchema();
    String sql =
        getDeviceQuerySql(
            deviceSchema, deviceQuery.getStartTimestamp(), deviceQuery.getEndTimestamp());
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("IoTDB:" + sql);
    }
    List<List<Object>> result = new ArrayList<>();
    try {
      ISessionDataSet sessionDataSet = sessionWrapper.executeQueryStatement(sql);
      while (sessionDataSet.hasNext()) {
        List<Object> line = new ArrayList<>();
        RowRecord rowRecord = sessionDataSet.next();
        line.add(rowRecord.getTimestamp());
        List<Field> fields = rowRecord.getFields();
        for (Field field : fields) {
          line.add(field.getStringValue());
        }
        result.add(line);
      }
      sessionDataSet.close();
    } catch (Exception e) {
      LOGGER.error("Query Error: " + sql + " exception:" + e.getMessage());
      return new Status(false, new TsdbException("Failed to query"), "Failed to query.");
    }

    return new Status(true, 0, sql, result);
  }

  @Override
  public DeviceSummary deviceSummary(DeviceQuery deviceQuery) throws TsdbException {
    DeviceSchema deviceSchema = deviceQuery.getDeviceSchema();
    int totalLineNumber;
    long minTimeStamp, maxTimeStamp;
    try {
      ISessionDataSet sessionDataSet =
          sessionWrapper.executeQueryStatement(getTotalLineNumberSql(deviceSchema));
      RowRecord rowRecord = sessionDataSet.next();
      totalLineNumber = Integer.parseInt(rowRecord.getFields().get(0).toString());
      sessionDataSet.close();

      sessionDataSet = sessionWrapper.executeQueryStatement(getMaxTimeStampSql(deviceSchema));
      rowRecord = sessionDataSet.next();
      maxTimeStamp = rowRecord.getTimestamp();
      sessionDataSet.close();

      sessionDataSet = sessionWrapper.executeQueryStatement(getMinTimeStampSql(deviceSchema));
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

  protected Tablet genTablet(IBatch batch) {
    config.getWORKLOAD_BUFFER_SIZE();
    List<MeasurementSchema> schemaList = new ArrayList<>();
    int sensorIndex = 0;
    for (Sensor sensor : batch.getDeviceSchema().getSensors()) {
      SensorType dataSensorType = sensor.getSensorType();
      schemaList.add(
          new MeasurementSchema(
              sensor.getName(),
              Enum.valueOf(TSDataType.class, dataSensorType.name),
              Enum.valueOf(TSEncoding.class, getEncodingType(dataSensorType))));
      sensorIndex++;
    }
    String deviceId = getDevicePath(batch.getDeviceSchema());
    Tablet tablet = new Tablet(deviceId, schemaList, batch.getRecords().size());
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    List<Sensor> sensors = batch.getDeviceSchema().getSensors();
    for (int recordIndex = 0; recordIndex < batch.getRecords().size(); recordIndex++) {
      tablet.rowSize++;
      Record record = batch.getRecords().get(recordIndex);
      sensorIndex = 0;
      long currentTime = record.getTimestamp();
      timestamps[recordIndex] = currentTime;
      for (int recordValueIndex = 0;
          recordValueIndex < record.getRecordDataValue().size();
          recordValueIndex++) {
        switch (sensors.get(sensorIndex).getSensorType()) {
          case BOOLEAN:
            boolean[] sensorsBool = (boolean[]) values[recordValueIndex];
            sensorsBool[recordIndex] =
                (boolean) (record.getRecordDataValue().get(recordValueIndex));
            break;
          case INT32:
            int[] sensorsInt = (int[]) values[recordValueIndex];
            sensorsInt[recordIndex] = (int) (record.getRecordDataValue().get(recordValueIndex));
            break;
          case INT64:
            long[] sensorsLong = (long[]) values[recordValueIndex];
            sensorsLong[recordIndex] = (long) (record.getRecordDataValue().get(recordValueIndex));
            break;
          case FLOAT:
            float[] sensorsFloat = (float[]) values[recordValueIndex];
            sensorsFloat[recordIndex] = (float) (record.getRecordDataValue().get(recordValueIndex));
            break;
          case DOUBLE:
            double[] sensorsDouble = (double[]) values[recordValueIndex];
            sensorsDouble[recordIndex] =
                (double) (record.getRecordDataValue().get(recordValueIndex));
            break;
          case TEXT:
            Binary[] sensorsText = (Binary[]) values[recordValueIndex];
            sensorsText[recordIndex] =
                binaryCache.computeIfAbsent(
                    (String) record.getRecordDataValue().get(recordValueIndex), Binary::valueOf);
            break;
          default:
            LOGGER.error("Unsupported Type: {}", sensors.get(sensorIndex).getSensorType());
        }
        sensorIndex++;
      }
    }
    return tablet;
  }

  public List<TSDataType> constructDataTypes(List<Sensor> sensors, int recordValueSize) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int sensorIndex = 0; sensorIndex < recordValueSize; sensorIndex++) {
      switch (sensors.get(sensorIndex).getSensorType()) {
        case BOOLEAN:
          dataTypes.add(TSDataType.BOOLEAN);
          break;
        case INT32:
          dataTypes.add(TSDataType.INT32);
          break;
        case INT64:
          dataTypes.add(TSDataType.INT64);
          break;
        case FLOAT:
          dataTypes.add(TSDataType.FLOAT);
          break;
        case DOUBLE:
          dataTypes.add(TSDataType.DOUBLE);
          break;
        case TEXT:
          dataTypes.add(TSDataType.TEXT);
          break;
      }
    }
    return dataTypes;
  }

  @Override
  public Status insertOneBatch(IBatch batch) {
    DBInsertMode insertMode = dbConfig.getDB_SWITCH().getInsertMode();
    switch (insertMode) {
      case INSERT_USE_SESSION_TABLET:
        return insertOneBatchByTablet(batch);
      case INSERT_USE_SESSION_RECORD:
        return insertOneBatchByRecord(batch);
      case INSERT_USE_SESSION_RECORDS:
        return insertOneBatchByRecords(batch);
      default:
        throw new IllegalStateException("Unexpected INSERT_MODE value: " + insertMode);
    }
  }

  Status waitWriteTaskToFinishAndGetStatus() {
    try {
      task.get(config.getWRITE_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      task.cancel(true);
      LOGGER.error("insertion failed", e);
      return new Status(false, 0, e, e.toString());
    }
    return new Status(true);
  }

  @Override
  public void close() throws TsdbException {
    try {
      if (sessionWrapper != null) {
        sessionWrapper.close();
      }
      if (ioTDBConnection != null) {
        ioTDBConnection.close();
      }
    } catch (IoTDBConnectionException e) {
      throw new TsdbException(e);
    } finally {
      this.service.shutdown();
    }
  }
}
