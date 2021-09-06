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

package cn.edu.tsinghua.iotdb.benchmark.victoriametrics;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class VictoriaMetrics implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(VictoriaMetrics.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

  private final String URL;
  private final String CREATE_URL;
  private final String DELETE_URL;
  private final String QUERY_URL;
  private final String QUERY_RANGE_URL;
  private DBConfig dbConfig;

  public VictoriaMetrics(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    URL = dbConfig.getHOST().get(0) + ":" + dbConfig.getPORT().get(0);
    CREATE_URL = URL + "/api/v1/import/prometheus?extra_label=db=" + dbConfig.getDB_NAME();
    DELETE_URL =
        URL
            + "/api/v1/admin/tsdb/delete_series?match%5B%5D=%7Bdb=%22"
            + dbConfig.getDB_NAME()
            + "%22%7D";
    QUERY_URL = URL + "/api/v1/query?query=";
    QUERY_RANGE_URL = URL + "/api/v1/query_range?query=";
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  @Override
  public void init() throws TsdbException {
    // no need to init
  }

  /**
   * Cleanup any state for this DB, including the old data deletion. Called once before each test if
   * IS_DELETE_DATA=true.
   */
  @Override
  public void cleanup() throws TsdbException {
    try {
      HttpRequestUtil.sendPost(DELETE_URL, null, "application/x-www-form-urlencoded");
      LOGGER.info("Clean Up finish!");
    } catch (Exception e) {
      LOGGER.warn("Failed to cleanup!");
      throw new TsdbException("Failed to cleanup!", e);
    }
  }

  /** Close the DB instance connections. Called once per DB instance. */
  @Override
  public void close() throws TsdbException {
    // no need to close
  }

  /**
   * Called once before each test if CREATE_SCHEMA=true.
   *
   * @param schemaList schema of devices to register
   */
  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    // no need to register
  }

  /**
   * Insert one batch into the database, the DB implementation needs to resolve the data in batch
   * which contains device schema and Map[Long, List[String]] records. The key of records is a
   * timestamp and the value is a list of sensor value data.
   *
   * @param batch universal insertion data structure
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    try {
      LinkedList<VictoriaMetricsModel> models = createDataModelByBatch(batch);
      StringBuffer body = new StringBuffer();
      for (VictoriaMetricsModel victoriaMetricsModel : models) {
        body.append(victoriaMetricsModel.toString() + "\n");
      }
      HttpRequestUtil.sendPost(
          CREATE_URL, body.toString(), "text/plain; version=0.0.4; charset=utf-8");
      return new Status(true);
    } catch (Exception e) {
      e.printStackTrace();
      return new Status(false, 0, e, e.toString());
    }
  }

  /**
   * Insert single-sensor one batch into the database, the DB implementation needs to resolve the
   * data in batch which contains device schema and Map[Long, List[String]] records. The key of
   * records is a timestamp and the value is one sensor value data.
   *
   * @param batch universal insertion data structure
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status insertOneSensorBatch(Batch batch) throws DBConnectException {
    try {
      LinkedList<VictoriaMetricsModel> models = createDataModelByBatch(batch);
      StringBuffer body = new StringBuffer();
      for (VictoriaMetricsModel victoriaMetricsModel : models) {
        body.append(victoriaMetricsModel.toString() + "\n");
      }
      HttpRequestUtil.sendPost(
          CREATE_URL, body.toString(), "text/plain; version=0.0.4; charset=utf-8");
      return new Status(true);
    } catch (Exception e) {
      e.printStackTrace();
      return new Status(false, 0, e, e.toString());
    }
  }

  private LinkedList<VictoriaMetricsModel> createDataModelByBatch(Batch batch) {
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    String device = deviceSchema.getDevice();
    List<Record> records = batch.getRecords();
    List<String> sensors = deviceSchema.getSensors();
    int sensorNum = sensors.size();
    LinkedList<VictoriaMetricsModel> models = new LinkedList<>();

    for (Record record : records) {
      if (batch.getColIndex() != -1) {
        // insert one line
        VictoriaMetricsModel model =
            createModel(
                deviceSchema.getGroup(),
                record.getTimestamp(),
                record.getRecordDataValue().get(0),
                device,
                deviceSchema.getSensors().get(batch.getColIndex()));
        models.addLast(model);
      } else {
        // insert align data
        for (int j = 0; j < sensorNum; j++) {
          VictoriaMetricsModel model =
              createModel(
                  deviceSchema.getGroup(),
                  record.getTimestamp(),
                  record.getRecordDataValue().get(j),
                  device,
                  deviceSchema.getSensors().get(j));
          models.addLast(model);
        }
      }
    }
    return models;
  }

  private VictoriaMetricsModel createModel(
      String metric, long timestamp, Object value, String device, String sensor) {
    VictoriaMetricsModel model = new VictoriaMetricsModel();
    model.setMetric(metric);
    model.setTimestamp(timestamp);
    model.setValue(value);
    model.setType(baseDataSchema.getSensorType(device, sensor));
    Map<String, String> tags = new HashMap<>();
    tags.put("device", device);
    tags.put("sensor", sensor);
    model.setTags(tags);
    return model;
  }

  /**
   * Query data of one or multiple sensors at a precise timestamp. /api/v1/query?query={db="test",
   * device="d_1", sensor="s_0"}&time=1609430405
   *
   * @param preciseQuery universal precise query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    List<DeviceSchema> deviceSchemas = preciseQuery.getDeviceSchema();
    int point = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        StringBuffer url = new StringBuffer(QUERY_URL);
        url.append(getMatch(deviceSchema.getDevice(), sensor));
        url.append("&").append("time=").append(preciseQuery.getTimestamp() / 1000);
        point += queryAndGetPoint(url.toString());
      }
    }
    return new Status(true, point);
  }

  /**
   * Query data of one or multiple sensors in a time range. /api/v1/query_range?query={db="test",
   * device="d_1", sensor="s_0"}&start=1609431250&end=1609431500
   *
   * @param rangeQuery universal range query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    List<DeviceSchema> deviceSchemas = rangeQuery.getDeviceSchema();
    int point = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        StringBuffer url = new StringBuffer(QUERY_RANGE_URL);
        url.append(getMatch(deviceSchema.getDevice(), sensor));
        url.append("&start=").append(rangeQuery.getStartTimestamp() / 1000);
        url.append("&end=").append(rangeQuery.getEndTimestamp() / 1000);
        point += queryAndGetPoint(url.toString());
      }
    }
    return new Status(true, point);
  }

  /**
   * Query data of one or multiple sensors in a time range with a value filter
   * /api/v1/query_range?query={db="test", device="d_1",
   * sensor="s_0"}>0&start=1609431250&end=1609431500
   *
   * @param valueRangeQuery contains universal range query with value filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    List<DeviceSchema> deviceSchemas = valueRangeQuery.getDeviceSchema();
    int point = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        StringBuffer url = new StringBuffer(QUERY_RANGE_URL);
        url.append(getMatch(deviceSchema.getDevice(), sensor));
        url.append("%3E").append(valueRangeQuery.getValueThreshold());
        url.append("&start=").append(valueRangeQuery.getStartTimestamp() / 1000);
        url.append("&end=").append(valueRangeQuery.getEndTimestamp() / 1000);
        point += queryAndGetPoint(url.toString());
      }
    }
    return new Status(true, point);
  }

  /**
   * Query aggregated data of one or multiple sensors in a time range using aggregation function.
   * /api/v1/query_range?query=count({db="test", device="d_1",
   * sensor="s_0"})&start=1609431250&end=1609431500
   *
   * @param aggRangeQuery contains universal aggregation query with time filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    List<DeviceSchema> deviceSchemas = aggRangeQuery.getDeviceSchema();
    int point = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        StringBuffer url = new StringBuffer(QUERY_RANGE_URL);
        url.append(aggRangeQuery.getAggFun()).append("%28");
        url.append(getMatch(deviceSchema.getDevice(), sensor)).append("%29");
        url.append("&start=").append(aggRangeQuery.getStartTimestamp() / 1000);
        url.append("&end=").append(aggRangeQuery.getEndTimestamp() / 1000);
        point += queryAndGetPoint(url.toString());
      }
    }
    return new Status(true, point);
  }

  /**
   * Query aggregated data of one or multiple sensors in the whole time range. e.g. select
   * func(v1)... from data where device in ? and value > ?
   * /api/v1/query_range?query=count({db="test", device="d_1", sensor="s_0"})&start=[start write
   * time]&end=[now]
   *
   * @param aggValueQuery contains universal aggregation query with value filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    List<DeviceSchema> deviceSchemas = aggValueQuery.getDeviceSchema();
    int point = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        StringBuffer url = new StringBuffer(QUERY_RANGE_URL);
        url.append(aggValueQuery.getAggFun()).append("%28");
        url.append(getMatch(deviceSchema.getDevice(), sensor)).append("%29");
        url.append("&start=").append(Constants.START_TIMESTAMP / 1000);
        url.append("&end=").append(System.currentTimeMillis() / 1000);
        point += queryAndGetPoint(url.toString());
      }
    }
    return new Status(true, point);
  }

  /**
   * Query aggregated data of one or multiple sensors with both time and value filters. e.g. select
   * func(v1)... from data where device in ? and time >= ? and time <= ? and value > ?
   * /api/v1/query_range?query=count({db="test", device="d_1",
   * sensor="s_0"}>0)&start=1609431250&end=1609431500
   *
   * @param aggRangeValueQuery contains universal aggregation query with time and value filters
   *     parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    List<DeviceSchema> deviceSchemas = aggRangeValueQuery.getDeviceSchema();
    int point = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        StringBuffer url = new StringBuffer(QUERY_RANGE_URL);
        url.append(aggRangeValueQuery.getAggFun()).append("%28");
        url.append(getMatch(deviceSchema.getDevice(), sensor)).append("%3E");
        url.append(aggRangeValueQuery.getValueThreshold()).append("%29");
        url.append("&start=").append(aggRangeValueQuery.getStartTimestamp() / 1000);
        url.append("&end=").append(aggRangeValueQuery.getEndTimestamp() / 1000);
        point += queryAndGetPoint(url.toString());
      }
    }
    return new Status(true, point);
  }

  /**
   * Query aggregated group-by-time data of one or multiple sensors within a time range.
   * api/v1/query_range?query=count({db="test", device="d_1", sensor="s_0"}[1ms]>0)&start=[start
   * write time]&end=[now]
   *
   * @param groupByQuery contains universal group by query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    List<DeviceSchema> deviceSchemas = groupByQuery.getDeviceSchema();
    int point = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        StringBuffer url = new StringBuffer(QUERY_RANGE_URL);
        url.append(groupByQuery.getAggFun()).append("%28");
        url.append(getMatch(deviceSchema.getDevice(), sensor)).append("%5B");
        url.append(groupByQuery.getGranularity()).append("ms%5D").append("%29");
        url.append("&start=").append(Constants.START_TIMESTAMP / 1000);
        url.append("&end=").append(System.currentTimeMillis() / 1000);
        point += queryAndGetPoint(url.toString());
      }
    }
    return new Status(true, point);
  }

  /**
   * Query the latest(max-timestamp) data of one or multiple sensors. e.g. select time, v1... where
   * device = ? and time = max(time)
   *
   * @param latestPointQuery contains universal latest point query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    List<DeviceSchema> deviceSchemas = latestPointQuery.getDeviceSchema();
    int point = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        StringBuffer url = new StringBuffer(QUERY_RANGE_URL);
        url.append("max_over_time%28");
        url.append(getMatch(deviceSchema.getDevice(), sensor)).append("%29");
        url.append("&start=").append(Constants.START_TIMESTAMP / 1000);
        url.append("&end=").append(System.currentTimeMillis() / 1000);
        point += queryAndGetPoint(url.toString());
      }
    }
    return new Status(true, point);
  }

  /**
   * similar to rangeQuery, but order by time desc.
   *
   * @param rangeQuery
   */
  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    List<DeviceSchema> deviceSchemas = rangeQuery.getDeviceSchema();
    int point = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        StringBuffer url = new StringBuffer(QUERY_RANGE_URL);
        url.append("sort_desc%28");
        url.append(getMatch(deviceSchema.getDevice(), sensor));
        url.append("%29");
        url.append("&start=").append(rangeQuery.getStartTimestamp() / 1000);
        url.append("&end=").append(rangeQuery.getEndTimestamp() / 1000);
        point += queryAndGetPoint(url.toString());
      }
    }
    return new Status(true, point);
  }

  /**
   * similar to rangeQuery, but order by time desc.
   *
   * @param valueRangeQuery
   */
  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    List<DeviceSchema> deviceSchemas = valueRangeQuery.getDeviceSchema();
    int point = 0;
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        StringBuffer url = new StringBuffer(QUERY_RANGE_URL);
        url.append("sort_desc%28");
        url.append(getMatch(deviceSchema.getDevice(), sensor));
        url.append("%3E").append(valueRangeQuery.getValueThreshold());
        url.append("%29");
        url.append("&start=").append(valueRangeQuery.getStartTimestamp() / 1000);
        url.append("&end=").append(valueRangeQuery.getEndTimestamp() / 1000);
        point += queryAndGetPoint(url.toString());
      }
    }
    return new Status(true, point);
  }

  /**
   * get selector
   *
   * @param device
   * @param sensor
   * @return
   */
  private String getMatch(String device, String sensor) {
    StringBuffer params = new StringBuffer();
    // change { to %7b " to %22 } to %7d
    params.append("%7b").append("db=%22").append(dbConfig.getDB_NAME()).append("%22");
    params.append(",device=%22").append(device).append("%22");
    params.append(",sensor=%22").append(sensor).append("%22").append("%7d");
    return params.toString();
  }

  /**
   * 执行查询并返回结果
   *
   * @param url
   * @return
   */
  private int queryAndGetPoint(String url) {
    int point = 0;
    try {
      String result = HttpRequestUtil.sendGet(url);
      JSONObject jsonObject = JSONObject.parseObject(result);
      point += ((JSONArray) ((JSONObject) jsonObject.get("data")).get("result")).size();
      return point;
    } catch (Exception e) {
      System.out.println("Failed get: " + url);
    }
    return 0;
  }
}
