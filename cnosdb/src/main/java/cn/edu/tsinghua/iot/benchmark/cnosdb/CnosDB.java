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

package cn.edu.tsinghua.iot.benchmark.cnosdb;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.influxdb.InfluxDB;
import cn.edu.tsinghua.iot.benchmark.influxdb.InfluxDataModel;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.VerificationQuery;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Response;
import org.influxdb.dto.BatchPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class CnosDB extends InfluxDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(CnosDB.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  private final String cnosUrl;
  private final String cnosDbName;

  private org.influxdb.InfluxDB influxDbInstance;
  private CnosConnection cnosConnection;
  private static final long TIMESTAMP_TO_NANO = getToNanoConst(config.getTIMESTAMP_PRECISION());

  /** constructor. */
  public CnosDB(DBConfig dbConfig) {
    super(dbConfig);
    cnosUrl = "http://" + dbConfig.getHOST().get(0) + ":" + dbConfig.getPORT().get(0);
    cnosDbName = dbConfig.getDB_NAME();
  }

  @Override
  public void init() throws TsdbException {
    try {
      Builder client =
          new Builder()
              .connectTimeout(5, TimeUnit.MINUTES)
              .readTimeout(5, TimeUnit.MINUTES)
              .writeTimeout(5, TimeUnit.MINUTES)
              .retryOnConnectionFailure(true);
      influxDbInstance = org.influxdb.InfluxDBFactory.connect(cnosUrl, client);
      cnosConnection = new CnosConnection(cnosUrl, cnosDbName);
    } catch (Exception e) {
      LOGGER.error("Initialize CnosDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try {
      Response response = cnosConnection.execute("DROP DATABASE IF EXISTS " + cnosDbName);
      response.close();
    } catch (Exception e) {
      LOGGER.error("Cleanup CnosDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    long start;
    long end;
    try {
      start = System.nanoTime();
      Response response =
          cnosConnection.execute(
              String.format(
                  "create database if not exists %s with shard %d",
                  cnosDbName, config.getCNOSDB_SHARD_NUMBER()));
      response.close();
      end = System.nanoTime();
    } catch (Exception e) {
      LOGGER.error("RegisterSchema CnosDB failed because" + e.getMessage());
      throw new TsdbException(e);
    }
    return TimeUtils.convertToSeconds(end - start, "ns");
  }

  @Override
  public Status insertOneBatch(IBatch batch) {
    BatchPoints batchPoints = BatchPoints.database(cnosDbName).build();
    try {
      InfluxDataModel model;
      for (Record record : batch.getRecords()) {
        model =
            createDataModel(
                batch.getDeviceSchema(), record.getTimestamp(), record.getRecordDataValue());
        batchPoints.point(model.toInfluxPoint());
      }

      influxDbInstance.write(batchPoints);

      return new Status(true);
    } catch (Exception e) {
      LOGGER.warn(e.getMessage());
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status groupByQueryOrderByDesc(GroupByQuery groupByQuery) {
    String sql = getAggQuerySqlHead(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
    sql = addWhereTimeClause(sql, groupByQuery);
    sql = addGroupByClause(sql, groupByQuery.getGranularity());
    return addTailClausesAndExecuteQueryAndGetStatus(sql);
  }

  @Override
  public Status addTailClausesAndExecuteQueryAndGetStatus(String sql) {
    if (config.getRESULT_ROW_LIMIT() >= 0) {
      sql += " limit " + config.getRESULT_ROW_LIMIT();
    }
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }

    try {
      long cnt = 0;
      Response response = cnosConnection.execute(sql);
      BufferedReader bufferedReader =
          new BufferedReader(new InputStreamReader(response.body().byteStream()));
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<String, Object> hashMap =
            objectMapper.readValue(line, new TypeReference<HashMap<String, Object>>() {});
        cnt += hashMap.size();
      }
      response.close();
      if (!config.isIS_QUIET_MODE()) {
        LOGGER.debug("{} 查到数据点数: {}", Thread.currentThread().getName(), cnt);
      }
      return new Status(true, cnt);
    } catch (Exception e) {
      LOGGER.error("Failed send http result, because" + e);
      return new Status(false, e, e.getMessage());
    }
  }

  /**
   * add group by clause for query.
   *
   * @param sqlHeader sql header
   * @param timeGranularity time granularity of group by
   */
  @Override
  public String addGroupByClause(String sqlHeader, long timeGranularity) {
    return sqlHeader + " GROUP BY date_bin(INTERVAL '" + timeGranularity + "' MILLISECOND, time)";
  }

  /**
   * generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT s_0, s_3 FROM root.group_0, root.group_1
   *     WHERE(device='d_0' OR device='d_1')
   */
  @Override
  public String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT CAST(time AS BIGINT) AS time, ");
    if (config.isALIGN_BY_DEVICE()) {
      builder.append("device, ");
    }
    List<Sensor> querySensors = devices.get(0).getSensors();

    builder.append(querySensors.get(0).getName());
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i).getName());
    }

    builder.append(generateConstrainForDevices(devices));
    return builder.toString();
  }

  /**
   * generate aggregation query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT count(s_0), count(s_3) FROM root.group_0, root.group_1
   *     WHERE(device='d_0' OR device='d_1')
   */
  @Override
  public String getAggQuerySqlHead(List<DeviceSchema> devices, String method) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    if (config.isALIGN_BY_DEVICE()) {
      builder.append("device, ");
    }
    List<Sensor> querySensors = devices.get(0).getSensors();

    if (Objects.equals(method, "last")) {
      builder.append(method).append("(time, ").append(querySensors.get(0).getName()).append(")");
    } else {
      builder.append(method).append("(").append(querySensors.get(0).getName()).append(")");
    }

    for (int i = 1; i < querySensors.size(); i++) {
      if (Objects.equals(method, "last")) {
        builder
            .append(", ")
            .append(method)
            .append("(time, ")
            .append(querySensors.get(i).getName())
            .append(")");
      } else {
        builder
            .append(", ")
            .append(method)
            .append("(")
            .append(querySensors.get(i).getName())
            .append(")");
      }
    }

    builder.append(generateConstrainForDevices(devices));
    return builder.toString();
  }

  /**
   * Using in verification
   *
   * @param verificationQuery
   */
  @Override
  public Status verificationQuery(VerificationQuery verificationQuery) {
    DeviceSchema deviceSchema = verificationQuery.getDeviceSchema();
    List<DeviceSchema> deviceSchemas = new ArrayList<>();
    deviceSchemas.add(deviceSchema);

    List<Record> records = verificationQuery.getRecords();
    if (records == null || records.isEmpty()) {
      return new Status(
          false,
          new TsdbException("There are no records in verficationQuery."),
          "There are no records in verficationQuery.");
    }

    StringBuilder sql = new StringBuilder();
    sql.append(getSimpleQuerySqlHead(deviceSchemas));
    Map<Long, List<Object>> recordMap = new HashMap<>();
    if (!deviceSchemas.isEmpty()) {
      sql.append("and (time = ").append(records.get(0).getTimestamp() * TIMESTAMP_TO_NANO);
    } else {
      sql.append("WHERE time = ").append(records.get(0).getTimestamp() * TIMESTAMP_TO_NANO);
    }
    recordMap.put(
        records.get(0).getTimestamp() * TIMESTAMP_TO_NANO, records.get(0).getRecordDataValue());
    for (int i = 1; i < records.size(); i++) {
      Record record = records.get(i);
      sql.append(" or time = ").append(record.getTimestamp() * TIMESTAMP_TO_NANO);
      recordMap.put(record.getTimestamp() * TIMESTAMP_TO_NANO, record.getRecordDataValue());
    }
    if (!deviceSchemas.isEmpty()) {
      sql.append(")");
    }

    try {
      long point = 0;
      int line = 0;
      Response response = cnosConnection.execute(sql.toString());
      BufferedReader bufferedReader =
          new BufferedReader(new InputStreamReader(response.body().byteStream()));
      String line_json;
      while ((line_json = bufferedReader.readLine()) != null) {
        ObjectMapper objectMapper = new ObjectMapper();
        LinkedHashMap<String, Object> resHashMap =
            objectMapper.readValue(
                line_json, new TypeReference<LinkedHashMap<String, Object>>() {});
        line++;

        Long time = (Long) resHashMap.get("time");
        List<Object> values = recordMap.get(time);

        for (int i = 0; i < values.size(); i++) {
          String target = String.valueOf(values.get(i));
          String result = resHashMap.get(deviceSchema.getSensors().get(i).getName()).toString();
          if (!result.equals(target)) {
            LOGGER.error("Using SQL: " + sql + ",Expected:" + result + " but was: " + target);
          } else {
            point++;
          }
        }
      }
      response.close();
      if (recordMap.size() != line) {
        LOGGER.error(
            "Using SQL: " + sql + ",Expected line:" + recordMap.size() + " but was: " + line);
      }
      if (!config.isIS_QUIET_MODE()) {
        LOGGER.debug("{} 查到数据点数: {}", Thread.currentThread().getName(), point);
      }
      return new Status(true, point);
    } catch (Exception e) {
      LOGGER.error("Failed verify query, because " + e);
      return new Status(false, e, e.getMessage());
    }
  }
}
