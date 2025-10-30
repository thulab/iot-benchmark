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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.Session.Builder;

import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TreeSessionManager extends SessionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSessionManager.class);
  private final Session session;

  /**
   * 新增：基于前缀路径的快速 last 查询（第11种查询方式）
   *
   * @param prefixPath 例如 Arrays.asList("root", "sg1")
   * @return SessionDataSet 查询结果
   */
  public SessionDataSet executeFastLastDataQueryForOnePrefixPath(List<String> prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    // 兼容未实现的 session 方法，优先用反射调用
    try {
      java.lang.reflect.Method method =
          session.getClass().getMethod("executeFastLastDataQueryForOnePrefixPath", List.class);
      Object result = method.invoke(session, prefixPath);
      return (SessionDataSet) result;
    } catch (NoSuchMethodException nsme) {
      LOGGER.error("Session未实现executeFastLastDataQueryForOnePrefixPath方法", nsme);
      throw new StatementExecutionException(
          "Session未实现executeFastLastDataQueryForOnePrefixPath方法", nsme);
    } catch (Exception e) {
      LOGGER.error("executeFastLastDataQueryForOnePrefixPath failed", e);
      throw new StatementExecutionException(e);
    }
  }

  public TreeSessionManager(DBConfig dbConfig) {
    super(dbConfig);
    List<String> hostUrls = new ArrayList<>(dbConfig.getHOST().size());
    for (int i = 0; i < dbConfig.getHOST().size(); i++) {
      hostUrls.add(dbConfig.getHOST().get(i) + ":" + dbConfig.getPORT().get(i));
    }
    this.session = buidlSession(hostUrls);
  }

  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    return session.executeQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws IoTDBConnectionException, StatementExecutionException {
    return session.executeQueryStatement(sql, timeoutInMs);
  }

  @Override
  protected void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    if (config.isVECTOR()) {
      session.insertAlignedRecord(deviceId, time, measurements, types, values);
    } else {
      session.insertRecord(deviceId, time, measurements, types, values);
    }
  }

  @Override
  protected void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    if (config.isVECTOR()) {
      session.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
    } else {
      session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
    }
  }

  @Override
  public void insertTablet(Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException {
    if (config.isVECTOR()) {
      session.insertAlignedTablet(tablet);
    } else {
      session.insertTablet(tablet);
    }
  }

  @Override
  public void open() {
    try {
      if (config.isENABLE_THRIFT_COMPRESSION()) {
        session.open(true);
      } else {
        session.open();
      }
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Failed to add session", e);
    }
  }

  @Override
  public void close() throws TsdbException {
    try {
      session.close();
    } catch (IoTDBConnectionException ioTDBConnectionException) {
      LOGGER.error("Failed to close Session because ");
      throw new TsdbException(ioTDBConnectionException);
    }
  }

  @Override
  public void createSchemaTemplate(Template template)
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    session.createSchemaTemplate(template);
  }

  @Override
  public void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    session.setStorageGroup(storageGroup);
  }

  @Override
  public void setSchemaTemplate(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {

    session.setSchemaTemplate(templateName, prefixPath);
  }

  @Override
  public void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
      throws IoTDBConnectionException, StatementExecutionException {
    session.createTimeseriesUsingSchemaTemplate(devicePathList);
  }

  @Override
  public void createAlignedTimeseries(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    session.createAlignedTimeseries(
        deviceId, measurements, dataTypes, encodings, compressors, measurementAliasList);
  }

  @Override
  public void createMultiTimeseries(
      List<String> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    session.createMultiTimeseries(
        paths,
        dataTypes,
        encodings,
        compressors,
        propsList,
        tagsList,
        attributesList,
        measurementAliasList);
  }

  public Session buidlSession(List<String> hostUrls) {
    return new Builder()
        .nodeUrls(hostUrls)
        .username(dbConfig.getUSERNAME())
        .password(dbConfig.getPASSWORD())
        .enableRedirection(true)
        .version(Version.V_1_0)
        .sqlDialect(config.getIoTDB_DIALECT_MODE().name())
        .enableAutoFetch(config.isENABLE_AUTO_FETCH())
        .enableIoTDBRpcCompression(config.isENABLE_IOTDB_RPC_COMPRESSION())
        .useSSL(config.isUSE_SSL())
        .trustStore(config.getTRUST_STORE_PATH())
        .trustStorePwd(config.getTRUST_STORE_PWD())
        .build();
  }
}
