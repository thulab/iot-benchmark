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

package cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.session.Session;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DMLStrategy {
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected ExecutorService service;
  protected Future<?> task;
  protected final DBConfig dbConfig;

  public DMLStrategy(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  public abstract Status insertOneBatch(IBatch batch, String devicePath) throws DBConnectException;

  public abstract long executeQueryAndGetStatusImpl(
      String executeSQL, Operation operation, AtomicBoolean isOk, List<List<Object>> records)
      throws SQLException;

  public abstract List<Integer> verificationQueryImpl(String sql, Map<Long, List<Object>> recordMap)
      throws Exception;

  public abstract List<List<Object>> deviceQueryImpl(String sql) throws Exception;

  public abstract DeviceSummary deviceSummary(
      String device, String totalLineNumberSql, String maxTimestampSql, String minTimestampSql)
      throws TsdbException, SQLException;

  public abstract void init() throws TsdbException;

  public abstract void cleanup();

  public abstract void close() throws TsdbException;

  protected Session buildSession(List<String> hostUrls, String dataBaseName) {
    return new Session.Builder()
        .nodeUrls(hostUrls)
        .username(dbConfig.getUSERNAME())
        .password(dbConfig.getPASSWORD())
        .enableRedirection(true)
        .version(Version.V_1_0)
        .sqlDialect(config.getIoTDB_DIALECT_MODE().name())
        .database(dataBaseName)
        .build();
  }
}
