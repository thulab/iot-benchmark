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

package cn.edu.tsinghua.iot.benchmark.dolphindb3;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

public class DolphinDB implements IDatabase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(DolphinDB.class);

  private static final String TABLE_NAME = "device_data";

  /** Guard so only one schema client creates the database+table. */
  private static final AtomicBoolean schemaInited = new AtomicBoolean(false);

  /** Guard so only one client drops the database in cleanup. */
  private static final AtomicBoolean cleanupDone = new AtomicBoolean(false);

  /** Schema clients wait on this so all of them return after schema is fully created. */
  private static final CyclicBarrier schemaBarrier =
      new CyclicBarrier(config.getSCHEMA_CLIENT_NUMBER());

  private final DBConfig dbConfig;
  private final String dbPath;

  private Connection jdbcConn;
  private MultithreadedTableWriter mtw;

  public DolphinDB(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    this.dbPath = "dfs://" + dbConfig.getDB_NAME();
  }

  @Override
  public void init() throws TsdbException {
    try {
      String url =
          String.format(
              "jdbc:dolphindb://%s:%s?user=%s&password=%s",
              dbConfig.getHOST().get(0),
              dbConfig.getPORT().get(0),
              dbConfig.getUSERNAME(),
              dbConfig.getPASSWORD());
      jdbcConn = DriverManager.getConnection(url);
    } catch (SQLException e) {
      LOGGER.error("Failed to connect DolphinDB", e);
      throw new TsdbException("Failed to connect DolphinDB", e);
    }
  }

  @Override
  public void close() throws TsdbException {
    if (mtw != null) {
      try {
        mtw.waitForThreadCompletion();
      } catch (Exception e) {
        LOGGER.warn("Failed to wait for MTW completion", e);
      }
    }
    if (jdbcConn != null) {
      try {
        jdbcConn.close();
      } catch (SQLException e) {
        LOGGER.warn("Failed to close DolphinDB JDBC connection", e);
      }
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    // implemented in Task 16
    throw new TsdbException("cleanup not implemented yet");
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    // implemented in Task 17
    throw new TsdbException("registerSchema not implemented yet");
  }

  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    // implemented in Task 19
    return new Status(false);
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return new Status(false);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    return new Status(false);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return new Status(false);
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return new Status(false);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return new Status(false);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return new Status(false);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return new Status(false);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return new Status(false);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return new Status(false);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return new Status(false);
  }

  @Override
  public String typeMap(SensorType iotdbSensorType) {
    switch (iotdbSensorType) {
      case BOOLEAN:
        return "BOOL";
      case INT32:
        return "INT";
      case INT64:
        return "LONG";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case TEXT:
      case STRING:
        return "STRING";
      case BLOB:
      case OBJECT:
        return "BLOB";
      case TIMESTAMP:
        return "TIMESTAMP";
      case DATE:
        return "DATE";
      default:
        LOGGER.warn("Unsupported sensorType {}, falling back to STRING.", iotdbSensorType);
        return "STRING";
    }
  }
}
