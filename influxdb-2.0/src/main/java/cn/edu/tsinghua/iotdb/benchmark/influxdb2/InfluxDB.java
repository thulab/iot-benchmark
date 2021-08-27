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
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class InfluxDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDB.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

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
      List<Organization> organizations = client.getOrganizationsApi().findOrganizations();
      for (Organization organization : organizations) {
        if (organization.getName().equals(org)) {
          client.getOrganizationsApi().deleteOrganization(organization);
        }
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
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    try {
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
    } catch (Exception e) {
      LOGGER.error("RegisterSchema InfluxDB failed because ", e);
      e.printStackTrace();
      throw new TsdbException(e);
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try {
      LinkedList<InfluxDBModel> influxDBModels = createDataModelByBatch(batch);
      List<String> lines = new ArrayList<>();
      for (InfluxDBModel influxDBModel : influxDBModels) {
        lines.add(model2write(influxDBModel));
      }
      HttpRequestUtil.sendPost(
          CREATE_URL, String.join("\n", lines), "text/plain; version=0.0.4; charset=utf-8", token);
      return new Status(true);
    } catch (Exception e) {
      return new Status(false, 0, e, e.getMessage());
    }
  }

  @Override
  public Status insertOneSensorBatch(Batch batch) {
    return null;
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
      for (Map.Entry<String, Object> pair : influxDBModel.getFields().entrySet()) {
        if (first) {
          first = false;
        } else {
          result.append(",");
        }
        result.append(pair.getKey());
        result.append("=");
        // get value
        String type =
            typeMap(
                baseDataSchema.getSensorType(influxDBModel.getTags().get("device"), pair.getKey()));
        switch (type) {
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
            LOGGER.error("Unsupported data type {}, use default data type: BINARY.", type);
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
    List<String> sensors = deviceSchema.getSensors();
    int sensorNum = sensors.size();
    LinkedList<InfluxDBModel> models = new LinkedList<>();

    for (Record record : records) {
      if (batch.getColIndex() != -1) {
        // insert one line
        InfluxDBModel model =
            createModel(
                deviceSchema.getGroup(),
                deviceSchema.getDevice(),
                record,
                deviceSchema.getSensors());
        models.addLast(model);
      } else {
        // insert align data
        for (int j = 0; j < sensorNum; j++) {
          InfluxDBModel model =
              createModel(
                  deviceSchema.getGroup(),
                  deviceSchema.getDevice(),
                  record,
                  deviceSchema.getSensors());
          models.addLast(model);
        }
      }
    }
    return models;
  }

  private InfluxDBModel createModel(
      String metric, String device, Record record, List<String> sensors) {
    InfluxDBModel model = new InfluxDBModel();
    model.setMetric(metric);
    model.setTimestamp(record.getTimestamp());

    model.addTag("device", device);

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
      for (String sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema.getGroup(),
                sensor,
                deviceSchema.getDevice(),
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
      for (String sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema.getGroup(),
                sensor,
                deviceSchema.getDevice(),
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
      for (String sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema.getGroup(),
                sensor,
                deviceSchema.getDevice(),
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

  private String getTimeSQLHeader(
      String group, String sensor, String device, long start, long end) {
    String sql =
        "from(bucket: \""
            + this.influxDbName
            + "\")\n"
            + "  |> range(start: "
            + start
            + ", stop:"
            + end
            + ")\n"
            + "  |> filter(fn: (r) => r[\"_measurement\"] == \""
            + group
            + "\")\n"
            + "  |> filter(fn: (r) => r[\"_field\"] == \""
            + sensor
            + "\")\n"
            + "  |> filter(fn: (r) => r[\"device\"] == \""
            + device
            + "\")";
    return sql;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    List<DeviceSchema> deviceSchemas = aggRangeQuery.getDeviceSchema();
    int result = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema.getGroup(),
                sensor,
                deviceSchema.getDevice(),
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
      for (String sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema.getGroup(),
                sensor,
                deviceSchema.getDevice(),
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
      for (String sensor : deviceSchema.getSensors()) {
        try {
          String sql =
              getTimeSQLHeader(
                  deviceSchema.getGroup(),
                  sensor,
                  deviceSchema.getDevice(),
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
      for (String sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema.getGroup(),
                sensor,
                deviceSchema.getDevice(),
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
      for (String sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema.getGroup(),
                sensor,
                deviceSchema.getDevice(),
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
      for (String sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema.getGroup(),
                sensor,
                deviceSchema.getDevice(),
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
      for (String sensor : deviceSchema.getSensors()) {
        String sql =
            getTimeSQLHeader(
                deviceSchema.getGroup(),
                sensor,
                deviceSchema.getDevice(),
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
    List<FluxTable> tables = client.getQueryApi().query(sql);
    for (FluxTable table : tables) {
      List<FluxRecord> fluxRecords = table.getRecords();
      cnt += fluxRecords.size();
    }
    return new Status(true, cnt);
  }
}
