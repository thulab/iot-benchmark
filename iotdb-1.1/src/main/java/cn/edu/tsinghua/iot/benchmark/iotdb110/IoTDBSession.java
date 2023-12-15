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
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;

import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.utils.NamedThreadFactory;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

public class IoTDBSession extends IoTDBSessionBase {
  private class BenchmarkSession implements IBenchmarkSession {
    private final Session session;

    public BenchmarkSession(Session session) {
      this.session = session;
    }

    @Override
    public void open() throws IoTDBConnectionException {
      session.open();
    }

    @Override
    public void open(boolean enableRPCCompression) throws IoTDBConnectionException {
      session.open(enableRPCCompression);
    }

    @Override
    public void insertRecord(
        String deviceId,
        long time,
        List<String> measurements,
        List<TSDataType> types,
        List<Object> values)
        throws IoTDBConnectionException, StatementExecutionException {
      session.insertRecord(deviceId, time, measurements, types, values);
    }

    @Override
    public void insertAlignedRecord(
        String multiSeriesId,
        long time,
        List<String> multiMeasurementComponents,
        List<TSDataType> types,
        List<Object> values)
        throws IoTDBConnectionException, StatementExecutionException {
      session.insertAlignedRecord(multiSeriesId, time, multiMeasurementComponents, types, values);
    }

    @Override
    public void insertRecords(
        List<String> deviceIds,
        List<Long> times,
        List<List<String>> measurementsList,
        List<List<TSDataType>> typesList,
        List<List<Object>> valuesList)
        throws IoTDBConnectionException, StatementExecutionException {
      session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
    }

    @Override
    public void insertAlignedRecords(
        List<String> multiSeriesIds,
        List<Long> times,
        List<List<String>> multiMeasurementComponentsList,
        List<List<TSDataType>> typesList,
        List<List<Object>> valuesList)
        throws IoTDBConnectionException, StatementExecutionException {
      session.insertAlignedRecords(
          multiSeriesIds, times, multiMeasurementComponentsList, typesList, valuesList);
    }

    @Override
    public void insertTablet(Tablet tablet)
        throws IoTDBConnectionException, StatementExecutionException {
      session.insertTablet(tablet);
    }

    @Override
    public void insertAlignedTablet(Tablet tablet)
        throws IoTDBConnectionException, StatementExecutionException {
      session.insertAlignedTablet(tablet);
    }

    @Override
    public ISessionDataSet executeQueryStatement(String sql)
        throws IoTDBConnectionException, StatementExecutionException {
      return new SessionDataSet1(session.executeQueryStatement(sql));
    }

    @Override
    public void close() throws IoTDBConnectionException {
      session.close();
    }

    @Override
    public void executeNonQueryStatement(String deleteSeriesSql)
        throws IoTDBConnectionException, StatementExecutionException {
      session.executeNonQueryStatement(deleteSeriesSql);
    }

    private class SessionDataSet1 implements ISessionDataSet {
      SessionDataSet sessionDataSet;

      public SessionDataSet1(SessionDataSet sessionDataSet) {
        this.sessionDataSet = sessionDataSet;
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

  public IoTDBSession(DBConfig dbConfig) {
    super(dbConfig);
    LOGGER = LoggerFactory.getLogger(IoTDBSession.class);
    List<String> hostUrls = new ArrayList<>(dbConfig.getHOST().size());
    for (int i = 0; i < dbConfig.getHOST().size(); i++) {
      hostUrls.add(dbConfig.getHOST().get(i) + ":" + dbConfig.getPORT().get(i));
    }
    sessionWrapper =
        new BenchmarkSession(
            new Session.Builder()
                .nodeUrls(hostUrls)
                .username(dbConfig.getUSERNAME())
                .password(dbConfig.getPASSWORD())
                .enableRedirection(true)
                .version(Version.V_1_0)
                .build());
  }

  @Override
  public void init() {
    try {
      if (config.isENABLE_THRIFT_COMPRESSION()) {
        sessionWrapper.open(true);
      } else {
        sessionWrapper.open();
      }
      this.service = Executors.newSingleThreadExecutor(new NamedThreadFactory("Session"));
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Failed to add session", e);
    }
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
