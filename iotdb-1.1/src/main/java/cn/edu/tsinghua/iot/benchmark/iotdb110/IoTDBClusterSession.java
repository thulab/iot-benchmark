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
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;

import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.NamedThreadFactory;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

public class IoTDBClusterSession extends IoTDBSessionBase {
  private class BenchmarkSessionPool implements IBenchmarkSession {
    private final SessionPool sessionPool;

    public BenchmarkSessionPool(
        List<String> hostUrls,
        String user,
        String password,
        int maxSize,
        boolean enableCompression,
        boolean enableRedirection) {
      this.sessionPool =
          new SessionPool(
              hostUrls,
              dbConfig.getUSERNAME(),
              dbConfig.getPASSWORD(),
              MAX_SESSION_CONNECTION_PER_CLIENT,
              config.isENABLE_THRIFT_COMPRESSION(),
              true);
    }

    @Override
    public void open() {}

    @Override
    public void open(boolean enableRPCCompression) {}

    @Override
    public void insertRecord(
        String deviceId,
        long time,
        List<String> measurements,
        List<TSDataType> types,
        List<Object> values)
        throws IoTDBConnectionException, StatementExecutionException {
      sessionPool.insertRecord(deviceId, time, measurements, types, values);
    }

    @Override
    public void insertAlignedRecord(
        String multiSeriesId,
        long time,
        List<String> multiMeasurementComponents,
        List<TSDataType> types,
        List<Object> values)
        throws IoTDBConnectionException, StatementExecutionException {
      sessionPool.insertAlignedRecord(
          multiSeriesId, time, multiMeasurementComponents, types, values);
    }

    @Override
    public void insertRecords(
        List<String> deviceIds,
        List<Long> times,
        List<List<String>> measurementsList,
        List<List<TSDataType>> typesList,
        List<List<Object>> valuesList)
        throws IoTDBConnectionException, StatementExecutionException {
      sessionPool.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
    }

    @Override
    public void insertAlignedRecords(
        List<String> multiSeriesIds,
        List<Long> times,
        List<List<String>> multiMeasurementComponentsList,
        List<List<TSDataType>> typesList,
        List<List<Object>> valuesList)
        throws IoTDBConnectionException, StatementExecutionException {
      sessionPool.insertAlignedRecords(
          multiSeriesIds, times, multiMeasurementComponentsList, typesList, valuesList);
    }

    @Override
    public void insertTablet(Tablet tablet)
        throws IoTDBConnectionException, StatementExecutionException {
      sessionPool.insertTablet(tablet);
    }

    @Override
    public void insertAlignedTablet(Tablet tablet)
        throws IoTDBConnectionException, StatementExecutionException {
      sessionPool.insertAlignedTablet(tablet);
    }

    @Override
    public ISessionDataSet executeQueryStatement(String sql)
        throws IoTDBConnectionException, StatementExecutionException {
      return new SessionDataSet2(sessionPool.executeQueryStatement(sql));
    }

    @Override
    public void close() {
      sessionPool.close();
    }

    @Override
    public void executeNonQueryStatement(String deleteSeriesSql)
        throws IoTDBConnectionException, StatementExecutionException {
      sessionPool.executeNonQueryStatement(deleteSeriesSql);
    }

    private class SessionDataSet2 implements ISessionDataSet {
      SessionDataSetWrapper sessionDataSet;

      public SessionDataSet2(SessionDataSetWrapper sessionDataSetWrapper) {
        this.sessionDataSet = sessionDataSetWrapper;
      }

      @Override
      public RowRecord next() throws IoTDBConnectionException, StatementExecutionException {
        return sessionDataSet.next();
      }

      @Override
      public boolean hasNext() throws IoTDBConnectionException, StatementExecutionException {
        return sessionDataSet.hasNext();
      }

      @Override
      public void close() throws IoTDBConnectionException, StatementExecutionException {
        sessionDataSet.close();
      }

      @Override
      public SessionDataSet.DataIterator iterator() {
        return sessionDataSet.iterator();
      }
    }
  }

  private static final int MAX_SESSION_CONNECTION_PER_CLIENT = 3;

  public IoTDBClusterSession(DBConfig dbConfig) {
    super(dbConfig);
    LOGGER = LoggerFactory.getLogger(IoTDBClusterSession.class);
    List<String> hostUrls = new ArrayList<>(dbConfig.getHOST().size());
    for (int i = 0; i < dbConfig.getHOST().size(); i++) {
      hostUrls.add(dbConfig.getHOST().get(i) + ":" + dbConfig.getPORT().get(i));
    }
    sessionWrapper =
        new BenchmarkSessionPool(
            hostUrls,
            dbConfig.getUSERNAME(),
            dbConfig.getPASSWORD(),
            MAX_SESSION_CONNECTION_PER_CLIENT,
            config.isENABLE_THRIFT_COMPRESSION(),
            true);
  }

  @Override
  public void init() throws TsdbException {
    // do nothing
    this.service = Executors.newSingleThreadExecutor(new NamedThreadFactory("ClusterSession"));
  }

  @Override
  public void cleanup() {
    try {
      sessionWrapper.executeNonQueryStatement(
          "drop database root." + config.getDbConfig().getDB_NAME() + ".**");
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Failed to connect to IoTDB:" + e.getMessage());
    } catch (StatementExecutionException e) {
      LOGGER.error("Failed to execute statement:" + e.getMessage());
    }

    try {
      sessionWrapper.executeNonQueryStatement("drop schema template " + config.getTEMPLATE_NAME());
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Failed to connect to IoTDB:" + e.getMessage());
    } catch (StatementExecutionException e) {
      LOGGER.error("Failed to execute statement:" + e.getMessage());
    }
  }
}
