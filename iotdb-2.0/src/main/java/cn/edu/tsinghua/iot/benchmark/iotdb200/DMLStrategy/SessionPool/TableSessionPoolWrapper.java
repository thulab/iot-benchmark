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

package cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy.SessionPool;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSessionPoolWrapper extends AbstractSessionPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSessionPoolWrapper.class);
  private final ITableSessionPool tableSessionPool;
  private boolean isFirstExecution = true;

  public TableSessionPoolWrapper(DBConfig dbConfig, Integer maxSize) {
    super(dbConfig);
    tableSessionPool = builderTableSessionPool(getHostUrls(), maxSize);
  }

  private ITableSessionPool builderTableSessionPool(List<String> hostUrls, Integer maxSize) {
    return new TableSessionPoolBuilder()
        .nodeUrls(hostUrls)
        .user(dbConfig.getUSERNAME())
        .password(dbConfig.getPASSWORD())
        .enableCompression(config.isENABLE_THRIFT_COMPRESSION())
        .enableRedirection(true)
        .maxSize(maxSize)
        .build();
  }

  /**
   * In TableSessionPool, the session obtained from the pool needs to be closed manually before it
   * can be returned to the session pool.
   *
   * @param sql
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   */
  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    ITableSession tableSession = tableSessionPool.getSession();
    tableSession.executeNonQueryStatement(sql);
    tableSession.close();
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    ITableSession tableSession = tableSessionPool.getSession();
    SessionDataSet sessionDataSet = tableSession.executeQueryStatement(sql);
    tableSession.close();
    return sessionDataSet;
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws TsdbException, IoTDBConnectionException, StatementExecutionException {
    ITableSession tableSession = tableSessionPool.getSession();
    SessionDataSet sessionDataSet = tableSession.executeQueryStatement(sql, timeoutInMs);
    tableSession.close();
    return sessionDataSet;
  }

  @Override
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void insertTablet(Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException {
    ITableSession tableSession = tableSessionPool.getSession();
    StringBuilder sql = new StringBuilder();
    sql.append("use ").append(dbConfig.getDB_NAME()).append("_").append(deviceSchema.getGroup());
    tableSession.executeNonQueryStatement(sql.toString());
    tableSession.insert(tablet);
    tableSession.close();
  }

  @Override
  public void open() {}

  @Override
  public void close() throws TsdbException {
    try {
      tableSessionPool.close();
    } catch (RuntimeException e) {
      LOGGER.error("Failed to close TableSession");
      throw new TsdbException(e);
    }
  }

  @Override
  public void createSchemaTemplate(Template template)
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void registerTable(HashMap<String, List<String>> tables)
      throws IoTDBConnectionException, StatementExecutionException {
    ITableSession tableSession = tableSessionPool.getSession();
    for (Map.Entry<String, List<String>> database : tables.entrySet()) {
      tableSession.executeNonQueryStatement("use " + database.getKey());
      for (String table : database.getValue()) {
        tableSession.executeNonQueryStatement(table);
      }
    }
    tableSession.close();
  }

  @Override
  public void setSchemaTemplate(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
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
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }
}
