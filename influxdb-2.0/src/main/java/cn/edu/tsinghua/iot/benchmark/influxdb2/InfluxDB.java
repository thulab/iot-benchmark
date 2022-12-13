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

package cn.edu.tsinghua.iot.benchmark.influxdb2;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.entity.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class InfluxDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDB.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  private final String token;
  private final String org;
  private String CREATE_URL = "http://%s/api/v2/write?org=%s&bucket=%s&precision=%s";

  private String influxUrl;
  private String influxDbName;
  private com.influxdb.client.InfluxDBClient client;

  /** constructor. */
  public InfluxDB(DBConfig dbConfig) {
    influxUrl = "http://" + dbConfig.getHOST().get(0) + ":" + dbConfig.getPORT().get(0);
    influxDbName = dbConfig.getDB_NAME();
    token = dbConfig.getTOKEN();
    org = dbConfig.getDB_NAME();
    CREATE_URL =
        String.format(
            CREATE_URL,
            dbConfig.getHOST().get(0) + ":" + dbConfig.getPORT().get(0),
            org,
            influxDbName,
            config.getTIMESTAMP_PRECISION());
  }

  @Override
  public void init() throws TsdbException {
    try {
      client = InfluxDBClientFactory.create(influxUrl, token.toCharArray(), org, influxDbName);
    } catch (Exception e) {
      LOGGER.error("Initialize InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try {
      Bucket bucket = client.getBucketsApi().findBucketByName(influxDbName);
      if (bucket == null) {
        LOGGER.warn("No bucket to clear!");
      } else {
        client.getBucketsApi().deleteBucket(bucket);
      }
    } catch (Exception e) {
      LOGGER.error("Cleanup InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    long start;
    long end;
    try {
      start = System.nanoTime();
      List<Organization> organizations = client.getOrganizationsApi().findOrganizations();
      String orgId = "";
      boolean isFind = false;
      for (Organization organization : organizations) {
        if (organization.getName().equals(org)) {
          orgId = organization.getId();
          isFind = true;
          break;
        }
      }
      if (!isFind) {
        Organization organization = client.getOrganizationsApi().createOrganization(org);
        orgId = organization.getId();
      }
      client.getBucketsApi().createBucket(influxDbName, orgId);
      end = System.nanoTime();
    } catch (Exception e) {
      LOGGER.error("RegisterSchema InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
    return TimeUtils.convertToSeconds(end - start, "ns");
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try {
      LinkedList<InfluxDBModel> influxDBModels = createDataModelByBatch(batch);
      List<String> lines = new ArrayList<>();
      for (InfluxDBModel influxDBModel : influxDBModels) {
        lines.add(model2write(influxDBModel));
      }
      int responseCode =
          HttpRequestUtil.writeData(
              CREATE_URL,
              String.join("\n", lines),
              "text/plain; version=0.0.4; charset=utf-8",
              token);
      if (HttpStatus.SC_NO_CONTENT == responseCode) {
        return new Status(true);
      } else {
        return new Status(false, 0);
      }
    } catch (Exception e) {
      return new Status(false, 0, e, e.getMessage());
    }
  }

  private String model2write(InfluxDBModel influxDBModel) {
    StringBuffer result = new StringBuffer(influxDBModel.getMetric());
    if (influxDBModel.getTags() != null) {
      for (Map.Entry<String, String> pair : influxDBModel.getTags().entrySet()) {
        result.append(",");
        result.append(pair.getKey());
        result.append("=");
        result.append(pair.getValue());
      }
    }
    result.append(" ");
    if (influxDBModel.getFields() != null) {
      boolean first = true;
      for (Map.Entry<Sensor, Object> pair : influxDBModel.getFields().entrySet()) {
        if (first) {
          first = false;
        } else {
          result.append(",");
        }
        result.append(pair.getKey());
        result.append("=");
        // get value
        String sensorType = typeMap(pair.getKey().getSensorType());
        switch (sensorType) {
          case "BOOLEAN":
            result.append(((boolean) pair.getValue()) ? "true" : "false");
            break;
          case "INT32":
            result.append((int) pair.getValue());
            break;
          case "INT64":
            result.append((long) pair.getValue());
            break;
          case "FLOAT":
            result.append((float) pair.getValue());
            break;
          case "DOUBLE":
            result.append((double) pair.getValue());
            break;
          case "TEXT":
            result.append("\"").append(pair.getValue()).append("\"");
            break;
          default:
            LOGGER.error(
                "Unsupported data sensorType {}, use default data sensorType: BINARY.", sensorType);
            return "TEXT";
        }
      }
    }
    result.append(" ");
    result.append(influxDBModel.getTimestamp());
    return result.toString();
  }

  private LinkedList<InfluxDBModel> createDataModelByBatch(Batch batch) {
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    List<Record> records = batch.getRecords();
    List<Sensor> sensors = deviceSchema.getSensors();
    LinkedList<InfluxDBModel> models = new LinkedList<>();
    Map<String, String> tags = batch.getDeviceSchema().getTags();

    for (Record record : records) {
      InfluxDBModel model =
          createModel(deviceSchema.getGroup(), deviceSchema.getDevice(), record, sensors, tags);
      models.addLast(model);
    }
    return models;
  }

  private InfluxDBModel createModel(
      String metric, String device, Record record, List<Sensor> sensors, Map<String, String> tags) {
    InfluxDBModel model = new InfluxDBModel();
    model.setMetric(metric);
    model.setTimestamp(record.getTimestamp());

    model.addTag("device", device);
    for (Map.Entry<String, String> pair : tags.entrySet()) {
      model.addTag(pair.getKey(), pair.getValue());
    }

    for (int i = 0; i < record.getRecordDataValue().size(); i++) {
      model.addField(sensors.get(i), record.getRecordDataValue().get(i));
    }
    return model;
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    List<DeviceSchema> deviceSchemas = preciseQuery.getDeviceSchema();
    int result = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema,
                sensor.getName(),
                preciseQuery.getTimestamp() / 1000,
                preciseQuery.getTimestamp() / 1000 + 1);
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
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema,
                sensor.getName(),
                rangeQuery.getStartTimestamp() / 1000,
                rangeQuery.getEndTimestamp() / 1000);
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
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema,
                sensor.getName(),
                valueRangeQuery.getStartTimestamp() / 1000,
                valueRangeQuery.getEndTimestamp() / 1000);
        sql +=
            "\n  |> filter(fn: (r) => r[\"_value\"] > " + valueRangeQuery.getValueThreshold() + ")";
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  private String getTimeSQLHeader(DeviceSchema deviceSchema, String sensor, long start, long end) {
    StringBuilder sql =
        new StringBuilder("from(bucket: \"").append(this.influxDbName).append("\")\n");
    sql.append("  |> range(start: ").append(start).append(", stop:").append(end).append(")\n");
    sql.append("  |> filter(fn: (r) => r[\"_measurement\"] == \"")
        .append(deviceSchema.getGroup())
        .append("\")\n");
    for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
      sql.append("  |> filter(fn: (r) => r[\"")
          .append(pair.getKey())
          .append("\"] == \"")
          .append(pair.getValue())
          .append("\")\n");
    }
    sql.append("  |> filter(fn: (r) => r[\"device\"] == \"")
        .append(deviceSchema.getDevice())
        .append("\")\n");
    sql.append("  |> filter(fn: (r) => r[\"_field\"] == \"").append(sensor).append("\")");
    return sql.toString();
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    List<DeviceSchema> deviceSchemas = aggRangeQuery.getDeviceSchema();
    int result = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema,
                sensor.getName(),
                aggRangeQuery.getStartTimestamp() / 1000,
                aggRangeQuery.getEndTimestamp() / 1000);
        String aggFun = aggRangeQuery.getAggFun();
        if (!aggFun.contains("()")) {
          aggFun += "()";
        }
        sql += "\n  |> " + aggFun;
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
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema,
                sensor.getName(),
                Constants.START_TIMESTAMP / 1000,
                System.currentTimeMillis() / 1000);
        // note that flux not support without range
        sql +=
            "\n  |> filter(fn: (r) => r[\"_value\"] > " + aggValueQuery.getValueThreshold() + ")";
        String aggFun = aggValueQuery.getAggFun();
        if (!aggFun.contains("()")) {
          aggFun += "()";
        }
        sql += "\n  |> " + aggFun;
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
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        try {
          String sql =
              getTimeSQLHeader(
                  deviceSchema,
                  sensor.getName(),
                  aggRangeValueQuery.getStartTimestamp() / 1000,
                  aggRangeValueQuery.getEndTimestamp() / 1000);
          sql +=
              "\n  |> filter(fn: (r) => r[\"_value\"] > "
                  + aggRangeValueQuery.getValueThreshold()
                  + ")";
          String aggFun = aggRangeValueQuery.getAggFun();
          if (!aggFun.contains("()")) {
            aggFun += "()";
          }
          sql += "\n  |> " + aggFun;
          Status status = executeQueryAndGetStatus(sql);
          result += status.getQueryResultPointNum();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return new Status(true, result);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    List<DeviceSchema> deviceSchemas = groupByQuery.getDeviceSchema();
    int result = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema,
                sensor.getName(),
                groupByQuery.getStartTimestamp() / 1000,
                groupByQuery.getEndTimestamp() / 1000);
        sql += "\n  |> integral(unit:" + groupByQuery.getGranularity() + "ms)";
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
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema,
                sensor.getName(),
                Constants.START_TIMESTAMP / 1000,
                System.currentTimeMillis() / 1000);
        sql += "\n  |> last(column: \"_time\")";
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    List<DeviceSchema> deviceSchemas = rangeQuery.getDeviceSchema();
    int result = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema,
                sensor.getName(),
                rangeQuery.getStartTimestamp() / 1000,
                rangeQuery.getEndTimestamp() / 1000);
        sql += "\n  |> sort(columns: [\"_time\"], desc: true)";
        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    List<DeviceSchema> deviceSchemas = valueRangeQuery.getDeviceSchema();
    int result = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema,
                sensor.getName(),
                valueRangeQuery.getStartTimestamp() / 1000,
                valueRangeQuery.getEndTimestamp() / 1000);
        sql +=
            "\n  |> filter(fn: (r) => r[\"_value\"] > " + valueRangeQuery.getValueThreshold() + ")";
        sql += "\n  |> sort(columns: [\"_time\"], desc: true)";

        Status status = executeQueryAndGetStatus(sql);
        result += status.getQueryResultPointNum();
      }
    }
    return new Status(true, result);
  }

  private Status executeQueryAndGetStatus(String sql) {
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.debug("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }
    int cnt = 0;
    List<FluxTable> tables = new ArrayList<>();
    try {
      tables = client.getQueryApi().query(sql);
    } catch (Exception e) {
      LOGGER.error("Error when query {} : {}", sql, e.getMessage());
    }
    for (FluxTable table : tables) {
      List<FluxRecord> fluxRecords = table.getRecords();
      cnt += fluxRecords.size();
    }
    return new Status(true, cnt);
  }
}
