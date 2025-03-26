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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

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
import java.util.List;
import java.util.Map;

public class TreeSessionPoolWrapper extends AbstractSessionPool {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TreeSessionPoolWrapper.class.getName());
  private final SessionPool sessionPool;

  public TreeSessionPoolWrapper(DBConfig dbConfig, Integer maxSize) {
    super(dbConfig);
    sessionPool = builderSessionPool(getHostUrls(), maxSize);
  }

  private SessionPool builderSessionPool(List<String> hostUrls, Integer maxSize) {
    return new SessionPool.Builder()
        .nodeUrls(hostUrls)
        .user(dbConfig.getUSERNAME())
        .password(dbConfig.getPASSWORD())
        .enableRedirection(true)
        .enableAutoFetch(false)
        .version(Version.V_1_0)
        .maxSize(maxSize)
        .waitToGetSessionTimeoutInMs(config.getREAD_OPERATION_TIMEOUT_MS())
        .build();
  }

  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.executeNonQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSetWrapper sessionDataSetWrapper = sessionPool.executeQueryStatement(sql);
    return sessionDataSetWrapper.getSessionDataSet();
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws TsdbException, IoTDBConnectionException, StatementExecutionException {
    SessionDataSetWrapper sessionDataSetWrapper =
        sessionPool.executeQueryStatement(sql, timeoutInMs);
    return sessionDataSetWrapper.getSessionDataSet();
  }

  @Override
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    if (config.isVECTOR()) {
      sessionPool.insertAlignedRecord(deviceId, time, measurements, types, values);
    } else {
      sessionPool.insertRecord(deviceId, time, measurements, types, values);
    }
  }

  @Override
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    if (config.isVECTOR()) {
      sessionPool.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
    } else {
      sessionPool.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
    }
  }

  @Override
  public void insertTablet(Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException {
    if (config.isVECTOR()) {
      sessionPool.insertAlignedTablet(tablet);
    } else {
      sessionPool.insertTablet(tablet);
    }
  }

  @Override
  public void open() {}

  @Override
  public void close() throws TsdbException {
    sessionPool.close();
  }

  @Override
  public void createSchemaTemplate(Template template)
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    sessionPool.createSchemaTemplate(template);
  }

  @Override
  public void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.createDatabase(storageGroup);
  }

  @Override
  public void setSchemaTemplate(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.setSchemaTemplate(templateName, prefixPath);
  }

  @Override
  public void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.createTimeseriesUsingSchemaTemplate(devicePathList);
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
    sessionPool.createAlignedTimeseries(
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
    sessionPool.createMultiTimeseries(
        paths,
        dataTypes,
        encodings,
        compressors,
        propsList,
        tagsList,
        attributesList,
        measurementAliasList);
  }
}
