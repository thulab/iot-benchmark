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

package cn.edu.tsinghua.iot.benchmark.opentsdb;

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
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class OpenTSDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenTSDB.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private final String queryUrl;
  private final String writeUrl;
  private final Random sensorRandom;
  private final Map<String, LinkedList<OpenTSDBDataModel>> dataMap = new HashMap<>();
  private final int backScanTime = 24;

  /** constructor. */
  public OpenTSDB(DBConfig dbConfig) {
    String[] split = config.getINSERT_DATATYPE_PROPORTION().split(":");
    if (split[0].equals("1") || split[split.length - 1].equals("1")) {
      System.out.println(
          "The Opentsdb does not support boolean and text data.\n"
              + "Please specify the property `INSERT_DATATYPE_PROPORTION` of configuration file to `0:1:1:1:1:0`.");
      System.exit(0);
    }
    sensorRandom = new Random(1 + config.getQUERY_SEED());
    String openUrl = dbConfig.getHOST().get(0) + ":" + dbConfig.getPORT().get(0);
    writeUrl = openUrl + "/api/put?summary ";
    queryUrl = openUrl + "/api/query";
  }

  @Override
  public void init() throws TsdbException {}

  @Override
  public void cleanup() throws TsdbException {
    // example JDBC_URL:
    // http://host:4242/api/query?start=2016/02/16-00:00:00&end=2016/02/17-23:59:59&m=avg:1ms-avg:metricname
    for (int i = 0; i < config.getGROUP_NUMBER(); i++) {
      String metric = "";
      String metricName = metric + "group_" + i;
      String DELETE_METRIC_URL = "%s?start=%s&m=sum:1ms-sum:%s";
      String deleteMetricURL =
          String.format(DELETE_METRIC_URL, queryUrl, Constants.START_TIMESTAMP, metricName);
      String response;
      try {
        response = HttpRequest.sendDelete(deleteMetricURL, "");
        LOGGER.info("Delete old data of {} ...", metricName);
        LOGGER.debug("Delete request response: {}", response);
      } catch (IOException e) {
        LOGGER.error("Delete old OpenTSDB metric {} failed. Error: {}", metricName, e.getMessage());
        throw new TsdbException(e);
      }
    }
  }

  // no need for opentsdb
  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    return 0.0;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try {
      // create dataModel
      LinkedList<OpenTSDBDataModel> models = createDataModelByBatch(batch);
      String sql = JSON.toJSONString(models);
      HttpRequest.sendPost(writeUrl, sql);
      return new Status(true);
    } catch (Exception e) {
      e.printStackTrace();
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    Map<String, Object> queryMap = new HashMap<>();
    List<Map<String, Object>> list = null;
    queryMap.put("msResolution", true);
    queryMap.put("start", preciseQuery.getTimestamp() - 1);
    queryMap.put("end", preciseQuery.getTimestamp() + 1);
    list = getSubQueries(preciseQuery.getDeviceSchema(), "none");
    queryMap.put("queries", list);
    String sql = JSON.toJSONString(queryMap);
    return executeQueryAndGetStatus(sql, false);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    Map<String, Object> queryMap = new HashMap<>();
    List<Map<String, Object>> list = null;
    queryMap.put("msResolution", true);
    queryMap.put("start", rangeQuery.getStartTimestamp() - 1);
    queryMap.put("end", rangeQuery.getEndTimestamp() + 1);
    list = getSubQueries(rangeQuery.getDeviceSchema(), "none");
    queryMap.put("queries", list);
    String sql = JSON.toJSONString(queryMap);
    return executeQueryAndGetStatus(sql, false);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    Exception e = new TsdbException("OpenTSDB don't support this kind of query");
    return new Status(false, 0, e, e.getMessage());
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    Map<String, Object> queryMap = new HashMap<>();
    List<Map<String, Object>> list = null;
    queryMap.put("msResolution", true);
    queryMap.put("start", aggRangeQuery.getStartTimestamp() - 1);
    queryMap.put("end", aggRangeQuery.getEndTimestamp() + 1);
    list = getSubQueries(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun());
    list.get(0).put("downsample", "0all-count");
    queryMap.put("queries", list);
    String sql = JSON.toJSONString(queryMap);
    return executeQueryAndGetStatus(sql, false);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    Exception e = new TsdbException("OpenTSDB don't support this kind of query");
    return new Status(false, 0, e, e.getMessage());
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    Exception e = new TsdbException("OpenTSDB don't support this kind of query");
    return new Status(false, 0, e, e.getMessage());
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    Map<String, Object> queryMap = new HashMap<>();
    List<Map<String, Object>> list = null;
    queryMap.put("msResolution", true);
    queryMap.put("start", groupByQuery.getStartTimestamp() - 1);
    queryMap.put("end", groupByQuery.getEndTimestamp() + 1);
    list = getSubQueries(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
    for (Map<String, Object> subQuery : list) {
      subQuery.put("downsample", groupByQuery.getGranularity() + "ms-" + groupByQuery.getAggFun());
    }
    queryMap.put("queries", list);
    String sql = JSON.toJSONString(queryMap);
    return executeQueryAndGetStatus(sql, false);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    Map<String, Object> queryMap = new HashMap<>();
    List<Map<String, Object>> list = null;
    queryMap.put("msResolution", true);
    queryMap.put("start", latestPointQuery.getStartTimestamp() - 1);
    queryMap.put("end", latestPointQuery.getEndTimestamp() + 1);
    list = getSubQueries(latestPointQuery.getDeviceSchema(), "none");
    for (Map<String, Object> subQuery : list) {
      subQuery.put("downsample", "0all-last");
    }
    queryMap.put("queries", list);
    String sql = JSON.toJSONString(queryMap);
    return executeQueryAndGetStatus(sql, true);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return null;
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return null;
  }

  @Override
  public void close() {}

  private LinkedList<OpenTSDBDataModel> createDataModelByBatch(Batch batch) throws TsdbException {
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    String device = deviceSchema.getDevice();
    List<Record> records = batch.getRecords();
    List<Sensor> sensors = deviceSchema.getSensors();
    int sensorNum = sensors.size();
    int recordNum = records.size();
    LinkedList<OpenTSDBDataModel> models = new LinkedList<>();

    for (int i = 0; i < recordNum; i++) {
      Record record = records.get(i);
      if (batch.getColIndex() != -1) {
        // 只插入一列
        OpenTSDBDataModel model = new OpenTSDBDataModel();
        model.setMetric(deviceSchema.getGroup());
        model.setTimestamp(record.getTimestamp());
        model.setValue(record.getRecordDataValue().get(0));
        Map<String, String> tags = new HashMap<>();
        tags.put("device", device);
        tags.put("sensor", deviceSchema.getSensors().get(batch.getColIndex()).getName());
        model.setTags(tags);
        models.addLast(model);
      } else {
        // 插入对齐数据
        for (int j = 0; j < sensorNum; j++) {
          OpenTSDBDataModel model = new OpenTSDBDataModel();
          model.setMetric(deviceSchema.getGroup());
          model.setTimestamp(record.getTimestamp());
          model.setValue(record.getRecordDataValue().get(j));
          Map<String, String> tags = new HashMap<>();
          tags.put("device", device);
          tags.put("sensor", deviceSchema.getSensors().get(j).getName());
          model.setTags(tags);
          models.addLast(model);
        }
      }
    }

    return models;
  }

  private Status executeQueryAndGetStatus(String sql, boolean isLatestPoint) {
    LOGGER.debug("{} query SQL: {}", Thread.currentThread().getName(), sql);
    try {
      String response;
      response = HttpRequest.sendPost(queryUrl, sql);
      long pointNum = getOneQueryPointNum(response, isLatestPoint);
      LOGGER.debug("{} 查到数据点数: {}", Thread.currentThread().getName(), pointNum);
      return new Status(true, pointNum);
    } catch (Exception e) {
      e.printStackTrace();
      return new Status(false, 0, e, sql);
    }
  }

  private long getOneQueryPointNum(String str, boolean isLatestPoint) {
    long pointNum = 0;
    if (!isLatestPoint) {
      JSONArray jsonArray = JSON.parseArray(str);
      for (int i = 0; i < jsonArray.size(); i++) {
        JSONObject json = (JSONObject) jsonArray.get(i);
        pointNum += json.getJSONObject("dps").size();
      }
    } else {
      JSONArray jsonArray = JSON.parseArray(str);
      pointNum += jsonArray.size();
    }
    return pointNum;
  }

  private List<Map<String, Object>> getSubQueries(List<DeviceSchema> devices, String aggreFunc) {
    List<Map<String, Object>> list = new ArrayList<>();

    List<String> sensorList = new ArrayList<>();
    for (Sensor sensor : devices.get(0).getSensors()) {
      sensorList.add(sensor.getName());
    }
    Collections.shuffle(sensorList, sensorRandom);
    // group2device
    Map<String, List<String>> metric2devices = new HashMap<>();
    for (DeviceSchema d : devices) {
      String m = d.getGroup();
      metric2devices.putIfAbsent(m, new ArrayList());
      metric2devices.get(m).add(d.getDevice());
    }

    for (Map.Entry<String, List<String>> queryMetric : metric2devices.entrySet()) {
      Map<String, Object> subQuery = new HashMap<>();
      subQuery.put("aggregator", aggreFunc);
      subQuery.put("metric", queryMetric.getKey());

      Map<String, String> tags = new HashMap<>();
      String deviceStr = "";
      for (String d : queryMetric.getValue()) {
        deviceStr += "|" + d;
      }
      deviceStr = deviceStr.substring(1);

      String sensorStr = sensorList.get(0);
      for (int i = 1; i < config.getQUERY_SENSOR_NUM(); i++) {
        sensorStr += "|" + sensorList.get(i);
      }
      tags.put("sensor", sensorStr);
      tags.put("device", deviceStr);
      subQuery.put("tags", tags);
      subQuery.put("downsample", "0all-count");
      list.add(subQuery);
    }
    return list;
  }
}
