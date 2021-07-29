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

package cn.edu.tsinghua.iotdb.benchmark.influxdb2;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class InfluxDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDB.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  private final String token = "ab9nQEU1Om9r-w9brgRNnIEMCkkW3ba108Q_zYdszjj9yuRmqjev_KgE4p-vFDeRCNDQuEqY1Gzlz2AtKWBX-w==";
  private final String org = "admin";

  private String influxUrl;
  private String influxDbName;
  private InfluxDBClient client;
  private WritePrecision writePrecision;

  private static final long TIMESTAMP_TO_NANO = getToNanoConst(config.getTIMESTAMP_PRECISION());

  /** constructor. */
  public InfluxDB() {
    influxUrl = config.getHOST().get(0) + ":" + config.getPORT().get(0);
    influxDbName = config.getDB_NAME();
  }

  @Override
  public void init() throws TsdbException {
    try {
      client = InfluxDBClientFactory.create(influxUrl, token.toCharArray(), org, influxDbName);
      switch (config.getTIMESTAMP_PRECISION()){
        case "ms":
          writePrecision = WritePrecision.MS;
          break;
        case "us":
          writePrecision = WritePrecision.US;
          break;
        case "ns":
          writePrecision = WritePrecision.NS;
          break;
        default:
          break;
      }
    } catch (Exception e) {
      LOGGER.error("Initialize InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try {
      Bucket bucket = client.getBucketsApi().findBucketByName(influxDbName);
      client.getBucketsApi().deleteBucket(bucket);
    } catch (Exception e) {
      LOGGER.error("Cleanup InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() {
    if(client != null){
      client.close();
    }
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    try {
      List<Organization> organizations = client.getOrganizationsApi().findOrganizations();
      String orgId = "";
      for(Organization organization: organizations){
        if(organization.getName().equals(org)) {
          orgId = organization.getId();
          break;
        }
      }
      client.getBucketsApi().createBucket(influxDbName, orgId);
    } catch (Exception e) {
      LOGGER.error("RegisterSchema InfluxDB failed because ", e);
      e.printStackTrace();
      throw new TsdbException(e);
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try {
      WriteApi writeApi = client.getWriteApi();
      LinkedList<InfluxDBModel> influxDBModels = createDataModelByBatch(batch);
      List<String> lines = new ArrayList<>();
      for(InfluxDBModel influxDBModel: influxDBModels){
        lines.add(influxDBModel.toString());
      }
      writeApi.writeRecords(writePrecision, lines);
      return new Status(true);
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.warn(e.getMessage());
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status insertOneSensorBatch(Batch batch) {
    try {
      WriteApi writeApi = client.getWriteApi();
      LinkedList<InfluxDBModel> influxDBModels = createDataModelByBatch(batch);
      for(InfluxDBModel influxDBModel: influxDBModels){
        writeApi.writeRecord(writePrecision, influxDBModel.toString());
      }
      return new Status(true);
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.warn(e.getMessage());
      return new Status(false, 0, e, e.toString());
    }
  }

  private LinkedList<InfluxDBModel> createDataModelByBatch(Batch batch) {
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    List<Record> records = batch.getRecords();
    List<String> sensors = deviceSchema.getSensors();
    int sensorNum = sensors.size();
    LinkedList<InfluxDBModel> models = new LinkedList<>();

    for (Record record : records) {
      if (batch.getColIndex() != -1) {
        // insert one line
        InfluxDBModel model =
                createModel(deviceSchema.getGroup(), deviceSchema.getDevice(),
                        record, deviceSchema.getSensors());
        models.addLast(model);
      } else {
        // insert align data
        for (int j = 0; j < sensorNum; j++) {
          InfluxDBModel model =
                  createModel(deviceSchema.getGroup(), deviceSchema.getDevice(),
                          record, deviceSchema.getSensors());
          models.addLast(model);
        }
      }
    }
    return models;
  }

  private InfluxDBModel createModel(String metric, String device, Record record, List<String> sensors){
    InfluxDBModel model = new InfluxDBModel();
    model.setMetric(metric);
    model.setTimestamp(record.getTimestamp());

    model.addTag("device", device);

    for(int i = 0; i < record.getRecordDataValue().size(); i++){
      model.addField(sensors.get(i), record.getRecordDataValue().get(i));
    }
    return model;
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    List<DeviceSchema> deviceSchemas = preciseQuery.getDeviceSchema();
    int result = 0;
    for(DeviceSchema deviceSchema: deviceSchemas){
      for(String sensor : deviceSchema.getSensors()){
        String sql = getTimeSQLHeader(deviceSchema.getGroup(), sensor,
                deviceSchema.getDevice(), preciseQuery.getTimestamp() / 1000, preciseQuery.getTimestamp() / 1000 + 1);
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    List<DeviceSchema> deviceSchemas = rangeQuery.getDeviceSchema();
    int result = 0;
    for(DeviceSchema deviceSchema: deviceSchemas){
      for(String sensor : deviceSchema.getSensors()){
        String sql = getTimeSQLHeader(deviceSchema.getGroup(), sensor,
                deviceSchema.getDevice(), rangeQuery.getStartTimestamp() / 1000, rangeQuery.getEndTimestamp() / 1000);
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    List<DeviceSchema> deviceSchemas = valueRangeQuery.getDeviceSchema();
    int result = 0;
    for(DeviceSchema deviceSchema: deviceSchemas){
      for(String sensor : deviceSchema.getSensors()){
        String sql = getTimeSQLHeader(deviceSchema.getGroup(), sensor,
                deviceSchema.getDevice(), valueRangeQuery.getStartTimestamp() / 1000, valueRangeQuery.getEndTimestamp() / 1000);
        sql += "\n  |> filter(fn: (r) => r[\"_value\"] > " + valueRangeQuery.getValueThreshold() + ")";
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  private String getTimeSQLHeader(String group, String sensor, String device, long start, long end){
    String sql = "from(bucket: \"" + this.influxDbName + "\")\n" +
            "  |> range(start: " + start + ", stop:" + end + ")\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + group + "\")\n" +
            "  |> filter(fn: (r) => r[\"_field\"] == \"" + sensor + "\")\n" +
            "  |> filter(fn: (r) => r[\"device\"] == \"" + device + "\")";
    return sql;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    List<DeviceSchema> deviceSchemas = aggRangeQuery.getDeviceSchema();
    int result = 0;
    for(DeviceSchema deviceSchema: deviceSchemas){
      for(String sensor : deviceSchema.getSensors()){
        String sql = getTimeSQLHeader(deviceSchema.getGroup(), sensor,
                deviceSchema.getDevice(), aggRangeQuery.getStartTimestamp() / 1000, aggRangeQuery.getEndTimestamp() / 1000);
        sql += "\n  |> " + aggRangeQuery.getAggFun();
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    List<DeviceSchema> deviceSchemas = aggValueQuery.getDeviceSchema();
    int result = 0;
    for(DeviceSchema deviceSchema: deviceSchemas){
      for(String sensor : deviceSchema.getSensors()){
        String sql = getTimeSQLHeader(deviceSchema.getGroup(), sensor,
                deviceSchema.getDevice(), Constants.START_TIMESTAMP / 1000, System.currentTimeMillis() / 1000);
        // note that flux not support without range
        sql += "\n  |> filter(fn: (r) => r[\"_value\"] > " + aggValueQuery.getValueThreshold() + ")";
        sql += "\n  |> " + aggValueQuery.getAggFun();
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    List<DeviceSchema> deviceSchemas = aggRangeValueQuery.getDeviceSchema();
    int result = 0;
    for(DeviceSchema deviceSchema: deviceSchemas){
      for(String sensor : deviceSchema.getSensors()){
        String sql = getTimeSQLHeader(deviceSchema.getGroup(), sensor,
                deviceSchema.getDevice(), aggRangeValueQuery.getStartTimestamp() / 1000, aggRangeValueQuery.getEndTimestamp() / 1000);
        sql += "\n  |> filter(fn: (r) => r[\"_value\"] > " + aggRangeValueQuery.getValueThreshold() + ")";
        sql += "\n  |> " + aggRangeValueQuery.getAggFun();
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  /**
   * eg. SELECT count(s_3) FROM group_4 WHERE ( device = 'd_16' ) AND time >= 1535558430000000000
   * AND time <=8680000000000 GROUP BY time(20000ms).
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    List<DeviceSchema> deviceSchemas = groupByQuery.getDeviceSchema();
    int result = 0;
    for(DeviceSchema deviceSchema: deviceSchemas){
      for(String sensor : deviceSchema.getSensors()){
        String sql = getTimeSQLHeader(deviceSchema.getGroup(), sensor,
                deviceSchema.getDevice(), groupByQuery.getStartTimestamp() / 1000, groupByQuery.getEndTimestamp() / 1000);
        sql += "\n  |> integral(unit:" + groupByQuery.getAggFun() +  ")";
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    List<DeviceSchema> deviceSchemas = latestPointQuery.getDeviceSchema();
    int result = 0;
    for(DeviceSchema deviceSchema: deviceSchemas){
      for(String sensor : deviceSchema.getSensors()){
        String sql = getTimeSQLHeader(deviceSchema.getGroup(), sensor,
                deviceSchema.getDevice(), Constants.START_TIMESTAMP / 1000, System.currentTimeMillis() / 1000);
        sql += "\n  |> last(column: \"_time\")";
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return null;
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return null;
  }

  private Status executeQueryAndGetStatus(String sql) {
    LOGGER.debug("{} query SQL: {}", Thread.currentThread().getName(), sql);
    int cnt = 0;
    List<FluxTable> tables = client.getQueryApi().query(sql);
    for(FluxTable table: tables){
      List<FluxRecord> fluxRecords = table.getRecords();
      cnt += fluxRecords.size();
    }
    return new Status(true, cnt);
  }

  private static String getPreciseQuerySql(PreciseQuery preciseQuery) {
    String strTime = "" + preciseQuery.getTimestamp() * TIMESTAMP_TO_NANO;
    return getSimpleQuerySqlHead(preciseQuery.getDeviceSchema()) + " AND time = " + strTime;
  }

  /**
   * add time filter for query statements.
   *
   * @param sql sql header
   * @param rangeQuery range query
   * @return sql with time filter
   */
  private static String addWhereTimeClause(String sql, RangeQuery rangeQuery) {
    String startTime = "" + rangeQuery.getStartTimestamp() * TIMESTAMP_TO_NANO;
    String endTime = "" + rangeQuery.getEndTimestamp() * TIMESTAMP_TO_NANO;
    return sql + " AND time >= " + startTime + " AND time <= " + endTime;
  }

  /**
   * add value filter for query statements.
   *
   * @param devices query device schema
   * @param sqlHeader sql header
   * @param valueThreshold lower bound of query value filter
   * @return sql with value filter
   */
  private static String addWhereValueClause(
      List<DeviceSchema> devices, String sqlHeader, double valueThreshold) {
    StringBuilder builder = new StringBuilder(sqlHeader);
    for (String sensor : devices.get(0).getSensors()) {
      builder.append(" AND ").append(sensor).append(" > ").append(valueThreshold);
    }
    return builder.toString();
  }

  /**
   * add group by clause for query.
   *
   * @param sqlHeader sql header
   * @param timeGranularity time granularity of group by
   */
  private static String addGroupByClause(String sqlHeader, long timeGranularity) {
    return sqlHeader + " GROUP BY time(" + timeGranularity + "ms)";
  }

  /**
   * generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT s_0, s_3 FROM root.group_0, root.group_1
   *     WHERE(device='d_0' OR device='d_1')
   */
  private static String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();

    builder.append(querySensors.get(0));
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i));
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
  private static String getAggQuerySqlHead(List<DeviceSchema> devices, String method) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();

    builder.append(method).append("(").append(querySensors.get(0)).append(")");
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(method).append("(").append(querySensors.get(i)).append(")");
    }

    builder.append(generateConstrainForDevices(devices));
    return builder.toString();
  }

  /**
   * generate from and where clause for specified devices.
   *
   * @param devices schema list of query devices
   * @return from and where clause
   */
  private static String generateConstrainForDevices(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    Set<String> groups = new HashSet<>();
    for (DeviceSchema d : devices) {
      groups.add(d.getGroup());
    }
    builder.append(" FROM ");
    for (String g : groups) {
      builder.append(g).append(" , ");
    }
    builder.deleteCharAt(builder.lastIndexOf(","));
    builder.append("WHERE (");
    for (DeviceSchema d : devices) {
      builder.append(" device = '").append(d.getDevice()).append("' OR");
    }
    builder.delete(builder.lastIndexOf("OR"), builder.length());
    builder.append(")");

    return builder.toString();
  }

  private static long getToNanoConst(String timePrecision) {
    if (timePrecision.equals("ms")) {
      return 1000000L;
    } else if (timePrecision.equals("us")) {
      return 1000L;
    } else {
      return 1L;
    }
  }
}
