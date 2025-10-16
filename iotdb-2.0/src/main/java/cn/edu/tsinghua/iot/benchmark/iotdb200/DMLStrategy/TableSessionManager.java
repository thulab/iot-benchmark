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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;

import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableSessionManager extends SessionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSessionManager.class);
  private final ITableSession tableSession;
  private boolean isFirstExecution = true;

  public TableSessionManager(DBConfig dbConfig) throws IoTDBConnectionException {
    super(dbConfig);
    List<String> hostUrls = new ArrayList<>(dbConfig.getHOST().size());
    for (int i = 0; i < dbConfig.getHOST().size(); i++) {
      hostUrls.add(dbConfig.getHOST().get(i) + ":" + dbConfig.getPORT().get(i));
    }
    this.tableSession = builderTableSession(hostUrls);
  }

  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    tableSession.executeNonQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    return tableSession.executeQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws IoTDBConnectionException, StatementExecutionException {
    return tableSession.executeQueryStatement(sql, timeoutInMs);
  }

  @Override
  protected void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values) {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  protected void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList) {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void insertTablet(Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException {
    if (isFirstExecution) {
      StringBuilder sql = new StringBuilder();
      sql.append("use ").append(dbConfig.getDB_NAME()).append("_").append(deviceSchema.getGroup());
      tableSession.executeNonQueryStatement(sql.toString());
      isFirstExecution = false;
    }
    tableSession.insert(tablet);
  }

  @Override
  public void open() {
    // TableSession combines the build and open operations of the session
  }

  @Override
  public void close() throws TsdbException {
    try {
      tableSession.close();
    } catch (IoTDBConnectionException ioTDBConnectionException) {
      LOGGER.error("Failed to close TableSession");
      throw new TsdbException(ioTDBConnectionException);
    }
  }

  @Override
  public void createSchemaTemplate(Template template) {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void setStorageGroup(String storageGroup) {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void setSchemaTemplate(String templateName, String prefixPath) {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void createTimeseriesUsingSchemaTemplate(List<String> devicePathList) {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void createAlignedTimeseries(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList) {
    throw new UnsupportedOperationException("TableSession does not implement this function");
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
      List<String> measurementAliasList) {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  private ITableSession builderTableSession(List<String> hostUrls) throws IoTDBConnectionException {
    return new TableSessionBuilder()
        .nodeUrls(hostUrls)
        .username(dbConfig.getUSERNAME())
        .password(dbConfig.getPASSWORD())
        .enableCompaction(config.isENABLE_THRIFT_COMPRESSION())
        .enableCompression(config.isENABLE_IOTDB_RPC_COMPRESSION())
        .enableRedirection(true)
        .enableAutoFetch(config.isENABLE_AUTO_FETCH())
        .useSSL(config.isUSE_SSL())
        .trustStore(config.getTRUST_STORE_PATH())
        .trustStorePwd(config.getTRUST_STORE_PWD())
        .build();
  }
}
