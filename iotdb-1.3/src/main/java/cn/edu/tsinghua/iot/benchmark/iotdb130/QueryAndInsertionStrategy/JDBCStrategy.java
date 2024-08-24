/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.iotdb130.QueryAndInsertionStrategy;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.iotdb130.IoTDB;
import cn.edu.tsinghua.iot.benchmark.iotdb130.SingleNodeJDBCConnection;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static cn.edu.tsinghua.iot.benchmark.client.operation.Operation.LATEST_POINT_QUERY;

public class JDBCStrategy extends IoTDBInsertionStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCStrategy.class);

  private SingleNodeJDBCConnection ioTDBConnection;

  public JDBCStrategy(DBConfig dbConfig) {
    super(dbConfig);
  }

  @Override
  public Status insertOneBatch(IBatch batch, String devicePath) throws DBConnectException {
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      for (Record record : batch.getRecords()) {
        String sql =
            getInsertOneBatchSql(
                batch.getDeviceSchema(), record.getTimestamp(), record.getRecordDataValue());
        statement.addBatch(sql);
      }
      statement.executeBatch();
      return new Status(true);
    } catch (Exception e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public long executeQueryAndGetStatusImpl(
      String executeSQL, Operation operation, AtomicBoolean isOk, List<List<Object>> records)
      throws SQLException {
    Boolean status = true;
    AtomicLong queryResultPointNum = new AtomicLong();
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      AtomicInteger line = new AtomicInteger();
      task =
          service.submit(
              () -> {
                try {
                  try (ResultSet resultSet = statement.executeQuery(executeSQL)) {
                    while (resultSet.next()) {
                      line.getAndIncrement();
                      if (config.isIS_COMPARISON()) {
                        List<Object> record = new ArrayList<>();
                        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                          switch (operation) {
                            case LATEST_POINT_QUERY:
                              if (i == 2 || i >= 4) {
                                continue;
                              }
                              break;
                            default:
                              break;
                          }
                          record.add(resultSet.getObject(i));
                        }
                        records.add(record);
                      }
                    }
                  }
                } catch (SQLException e) {
                  LOGGER.error("exception occurred when execute query={}", executeSQL, e);
                  isOk.set(false);
                }
                long resultPointNum = line.get();
                if (!LATEST_POINT_QUERY.equals(operation)) {
                  resultPointNum *= config.getQUERY_SENSOR_NUM();
                  resultPointNum *= config.getQUERY_DEVICE_NUM();
                }
                queryResultPointNum.set(resultPointNum);
              });
    }
    try {
      task.get(config.getREAD_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      task.cancel(true);
    }
    return queryResultPointNum.get();
  }

  @Override
  public List<Integer> verificationQueryImpl(String sql, Map<Long, List<Object>> recordMap)
      throws Exception {
    int point = 0, line = 0;
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      while (resultSet.next()) {
        long timeStamp = resultSet.getLong(1);
        List<Object> values = recordMap.get(timeStamp);
        for (int i = 0; i < values.size(); i++) {
          String value = resultSet.getString(i + 2);
          String target = String.valueOf(values.get(i));
          if (!value.equals(target)) {
            LOGGER.error("Using SQL: " + sql + ",Expected:" + value + " but was: " + target);
          } else {
            point++;
          }
        }
        line++;
      }
    }
    return Arrays.asList(point, line);
  }

  @Override
  public List<List<Object>> deviceQueryImpl(String sql) throws Exception {
    List<List<Object>> result = new ArrayList<>();
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int colNumber = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        List<Object> line = new ArrayList<>();
        for (int i = 1; i <= colNumber; i++) {
          line.add(resultSet.getObject(i));
        }
        result.add(line);
      }
    }
    return result;
  }

  @Override
  public DeviceSummary deviceSummary(
      String device, String totalLineNumberSql, String maxTimestampSql, String minTimestampSql)
      throws SQLException, TsdbException {
    int totalLineNumber = 0;
    long minTimeStamp = 0, maxTimeStamp = 0;
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      ResultSet resultSet = statement.executeQuery(totalLineNumberSql);
      resultSet.next();
      totalLineNumber = Integer.parseInt(resultSet.getString(1));

      resultSet = statement.executeQuery(maxTimestampSql);
      resultSet.next();
      maxTimeStamp = Long.parseLong(resultSet.getObject(1).toString());

      resultSet = statement.executeQuery(minTimestampSql);
      resultSet.next();
      minTimeStamp = Long.parseLong(resultSet.getObject(1).toString());
    }
    return new DeviceSummary(device, totalLineNumber, minTimeStamp, maxTimeStamp);
  }

  @Override
  public void init() throws TsdbException {
    if (ioTDBConnection == null) {
      try {
        ioTDBConnection = new SingleNodeJDBCConnection(dbConfig);
        ioTDBConnection.init();
        this.service =
            Executors.newSingleThreadExecutor(new NamedThreadFactory("DataClientExecuteJob"));
      } catch (Exception e) {
        throw new TsdbException(e);
      }
    }
  }

  @Override
  public void cleanup() {
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      statement.execute(IoTDB.DELETE_SERIES_SQL);
      LOGGER.info("Finish clean data!");
    } catch (Exception e) {
      LOGGER.warn("No Data to Clean!");
    }
  }

  @Override
  public void close() throws TsdbException {
    if (ioTDBConnection != null) {
      ioTDBConnection.close();
    }
    if (service != null) {
      service.shutdownNow();
    }
    if (task != null) {
      task.cancel(true);
    }
  }

  // region private method
  private String getInsertOneBatchSql(
      DeviceSchema deviceSchema, long timestamp, List<Object> values) {
    StringBuilder builder = new StringBuilder("insert into ");
    builder.append(deviceSchema.getDevicePath()).append("(timestamp");
    for (Sensor sensor : deviceSchema.getSensors()) {
      builder.append(",").append(sensor.getName());
    }
    if (config.isVECTOR()) {
      builder.append(") aligned values(");
    } else {
      builder.append(") values(");
    }
    builder.append(timestamp);
    int sensorIndex = 0;
    List<Sensor> sensors = deviceSchema.getSensors();
    for (Object value : values) {
      switch (sensors.get(sensorIndex).getSensorType()) {
        case BOOLEAN:
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case TIMESTAMP:
        case DATE:
          builder.append(",").append(value);
          break;
        case TEXT:
        case STRING:
          builder.append(",").append("'").append(value).append("'");
          break;
        case BLOB:
          builder.append(",").append("X'").append(value).append("'");
          break;
      }
      sensorIndex++;
    }
    builder.append(")");
    LOGGER.debug("getInsertOneBatchSql: {}", builder);
    return builder.toString();
  }
}
