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

package cn.edu.tsinghua.iot.benchmark.measurement.persistence.iotdb;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.measurement.enums.Metric;
import cn.edu.tsinghua.iot.benchmark.measurement.enums.SingleTestMetrics;
import cn.edu.tsinghua.iot.benchmark.measurement.enums.SystemMetrics;
import cn.edu.tsinghua.iot.benchmark.measurement.enums.TotalOperationResult;
import cn.edu.tsinghua.iot.benchmark.measurement.enums.TotalResult;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.TestDataPersistence;
import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Map;

public class IotdbRecorder extends TestDataPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(IotdbRecorder.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String JDBC_URL = "jdbc:iotdb://%s:%s/";
  private static final SimpleDateFormat projectDateFormat =
      new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss");
  private static final long EXP_TIME = System.currentTimeMillis();

  private static final String CREATE_SERIES_SQL =
      "CREATE TIMESERIES %s WITH DATATYPE=%s,ENCODING=%s,COMPRESSOR=%s";
  private static final String PROJECT_ID =
      String.format(
          "%s_%s", config.getREMARK(), projectDateFormat.format(new java.util.Date(EXP_TIME)));
  private static final String PATH_PREFIX = "root." + config.getTEST_DATA_STORE_DB();
  private static final String INSERT_SQL_PREFIX = "INSERT INTO " + PATH_PREFIX;
  private static final String OPERATION_RESULT_PREFIX = INSERT_SQL_PREFIX + "." + PROJECT_ID + ".";
  private static final String INSERT_SQL_STR1 = ") values(";
  private static final String INSERT_SQL_STR2 = "(timestamp";

  private static final String ENCODING = "PLAIN";
  private static final String COMPRESS = "SNAPPY";
  private static final String DOUBLE_TYPE = "DOUBLE";
  private static final int SEND_TO_IOTDB_BATCH_SIZE = 1000;

  private static final String ALREADY_KEYWORD = "already exist";
  private static final String CRETE_SCHEMA_ERROR_HINT = "create schema error";

  private String localName;
  private Connection connection;
  private Statement globalStatement;

  private long count = 0;

  private static int threadID = 0;
  private int myID = 0;
  public static boolean initSchema = false;

  public IotdbRecorder() {
    try {
      InetAddress localhost = InetAddress.getLocalHost();
      localName = localhost.getHostName();
    } catch (UnknownHostException e) {
      localName = "localName";
      LOGGER.error("Get localhost failed because: {}", e.getMessage(), e);
    }
    localName = localName.replace("-", "_");
    localName = localName.replace(".", "_");
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      connection =
          DriverManager.getConnection(
              String.format(
                  JDBC_URL, config.getTEST_DATA_STORE_IP(), config.getTEST_DATA_STORE_PORT()),
              config.getTEST_DATA_STORE_USER(),
              config.getTEST_DATA_STORE_PW());
      synchronized (this) {
        if (!initSchema) {
          initSchema();
          initSchema = true;
        }
      }
      this.setThreadID();
      globalStatement = connection.createStatement();
    } catch (Exception e) {
      LOGGER.error("Initialize IoTDB failed because ", e);
    }
  }

  private void initSchema() {
    // create time series
    if (config.getBENCHMARK_WORK_MODE() == BenchmarkMode.TEST_WITH_DEFAULT_PATH) {
      initSingleTestMetrics();
      initResultMetrics();
    }
    if (config.getBENCHMARK_WORK_MODE() == BenchmarkMode.SERVER) {
      initSystemMetrics();
    }
  }

  /** System metrics include: root.test.localName. */
  private void initSystemMetrics() {
    try (Statement statement = connection.createStatement()) {
      for (SystemMetrics systemMetric : SystemMetrics.values()) {
        String createSeriesSql =
            String.format(
                CREATE_SERIES_SQL,
                PATH_PREFIX + "." + localName + "." + PROJECT_ID + "." + systemMetric,
                DOUBLE_TYPE,
                ENCODING,
                COMPRESS);
        statement.addBatch(createSeriesSql);
      }
      statement.executeBatch();
      statement.clearBatch();
    } catch (SQLException e) {
      // ignore if already has the time series
      if (!e.getMessage().contains(ALREADY_KEYWORD)) {
        LOGGER.error(CRETE_SCHEMA_ERROR_HINT, e);
      }
    }
  }

  private void initResultMetrics() {
    try (Statement statement = connection.createStatement()) {
      for (Operation op : Operation.values()) {
        for (Metric metric : Metric.values()) {
          String createSeriesSql =
              String.format(
                  CREATE_SERIES_SQL,
                  PATH_PREFIX + "." + op + "." + metric.getName(),
                  DOUBLE_TYPE,
                  ENCODING,
                  COMPRESS);
          statement.addBatch(createSeriesSql);
        }
        for (TotalOperationResult totalOperationResult : TotalOperationResult.values()) {
          String createSeriesSql =
              String.format(
                  CREATE_SERIES_SQL,
                  PATH_PREFIX + "." + op + "." + totalOperationResult.getName(),
                  DOUBLE_TYPE,
                  ENCODING,
                  COMPRESS);
          statement.addBatch(createSeriesSql);
        }
      }
      for (TotalResult totalResult : TotalResult.values()) {
        String createSeriesSql =
            String.format(
                CREATE_SERIES_SQL,
                PATH_PREFIX + ".total" + "." + totalResult.getName(),
                DOUBLE_TYPE,
                ENCODING,
                COMPRESS);
        statement.addBatch(createSeriesSql);
      }
      statement.executeBatch();
      statement.clearBatch();
    } catch (SQLException e) {
      // ignore if already has the time series
      if (!e.getMessage().contains(ALREADY_KEYWORD)) {
        LOGGER.error(CRETE_SCHEMA_ERROR_HINT, e);
      }
    }
  }

  private void initSingleTestMetrics() {
    try (Statement statement = connection.createStatement()) {
      for (SingleTestMetrics metrics : SingleTestMetrics.values()) {
        for (Operation op : Operation.values()) {
          String createSeriesSql =
              String.format(
                  CREATE_SERIES_SQL,
                  PATH_PREFIX + "." + PROJECT_ID + "." + op.getName() + "." + metrics.getName(),
                  metrics.getType(),
                  ENCODING,
                  COMPRESS);
          statement.addBatch(createSeriesSql);
        }
      }
      statement.executeBatch();
      statement.clearBatch();
    } catch (SQLException e) {
      // ignore if already has the time series
      if (!e.getMessage().contains(ALREADY_KEYWORD)) {
        LOGGER.error(CRETE_SCHEMA_ERROR_HINT, e);
      }
    }
  }

  @Override
  public void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap) {
    try (Statement statement = connection.createStatement()) {
      long currTime = System.currentTimeMillis();
      currTime = currTime * 1000000;
      StringBuffer builder =
          new StringBuffer(INSERT_SQL_PREFIX)
              .append(".")
              .append(localName)
              .append(".")
              .append(PROJECT_ID)
              .append(INSERT_SQL_STR2);
      StringBuffer valueBuilder = new StringBuffer(INSERT_SQL_STR1).append(currTime);
      for (Map.Entry entry : systemMetricsMap.entrySet()) {
        builder.append(",").append(entry.getKey());
        if (entry.getValue() == null) {
          valueBuilder.append(",").append(0);
        } else {
          valueBuilder.append(",").append(entry.getValue());
        }
      }
      builder.append(valueBuilder).append(")");
      statement.execute(builder.toString());
    } catch (SQLException e) {
      LOGGER.error("Insert system metric data failed ", e);
    }
  }

  @Override
  protected void saveOperationResult(
      String operation,
      long okPoint,
      long failPoint,
      double latency,
      String remark,
      String device) {
    StringBuffer builder = new StringBuffer(OPERATION_RESULT_PREFIX);
    long currTime = System.currentTimeMillis();
    currTime = currTime * 1000000 + this.getThreadID();
    builder.append(operation).append(INSERT_SQL_STR2);
    for (SingleTestMetrics metrics : SingleTestMetrics.values()) {
      builder.append(",").append(metrics.getName());
    }
    builder.append(INSERT_SQL_STR1);
    builder.append(currTime);
    builder.append(",'").append(device).append("'");
    builder.append(",").append(okPoint);
    builder.append(",").append(failPoint);
    builder.append(",").append(latency);
    builder.append(",'").append(remark).append("'");
    addBatch(builder);
  }

  @Override
  protected void createNewRecord(
      String operation,
      long okPoint,
      long failPoint,
      double latency,
      String remark,
      String device) {
    // Do nothing
  }

  @Override
  protected void saveResult(String operation, String key, String value) {
    StringBuffer builder = new StringBuffer(INSERT_SQL_PREFIX);
    builder.append(".").append(operation).append(INSERT_SQL_STR2);
    builder.append(",").append(key);
    builder.append(INSERT_SQL_STR1);
    long currTime = EXP_TIME * 1000000;
    builder.append(currTime);
    builder.append(",").append(value);
    addBatch(builder);
  }

  private void addBatch(StringBuffer builder) {
    builder.append(")");
    try {
      globalStatement.addBatch(builder.toString());
      count++;
      if (count % SEND_TO_IOTDB_BATCH_SIZE == 0) {
        globalStatement.executeBatch();
        globalStatement.clearBatch();
      }
    } catch (SQLException e) {
      LOGGER.error("Add batch failed", e);
    }
  }

  @Override
  public void saveTestConfig() {
    // TODO save config into IoTDB
  }

  public void setThreadID() {
    synchronized (this) {
      myID = threadID++;
    }
  }

  public int getThreadID() {
    return myID;
  }

  @Override
  public void close() {
    try {
      if (!connection.isClosed()) {
        globalStatement.executeBatch();
        globalStatement.clearBatch();
        globalStatement.close();
        connection.close();
      }
    } catch (SQLException e) {
      LOGGER.error("close failed", e);
    }
  }
}
