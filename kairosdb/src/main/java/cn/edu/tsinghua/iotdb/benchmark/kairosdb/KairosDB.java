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

package cn.edu.tsinghua.iotdb.benchmark.kairosdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import com.alibaba.fastjson.JSON;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;
import org.kairosdb.client.builder.AggregatorFactory.FilterOperation;
import org.kairosdb.client.builder.aggregator.SamplingAggregator;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.QueryResult;
import org.kairosdb.client.response.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

public class KairosDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(KairosDB.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private String writeUrl;
  private HttpClient client;
  private DBConfig dbConfig;

  private static final String GROUP_STR = "group";
  private static final String DEVICE_STR = "device";

  public KairosDB(DBConfig dbConfig) {
    writeUrl =
        "http://"
            + dbConfig.getHOST().get(0)
            + ":"
            + dbConfig.getPORT().get(0)
            + "/api/v1/datapoints";
    this.dbConfig = dbConfig;
  }

  @Override
  public void init() throws TsdbException {
    try {
      client =
          new HttpClient("http://" + dbConfig.getHOST().get(0) + ":" + dbConfig.getPORT().get(0));
    } catch (MalformedURLException e) {
      e.printStackTrace();
      throw new TsdbException(
          "Init KairosDB client failed, the url is "
              + dbConfig.getHOST()
              + ":"
              + dbConfig.getPORT()
              + ". Message is "
              + e.getMessage());
    }
  }

  @Override
  public void cleanup() {
    try {
      for (String metric : client.getMetricNames()) {
        // skip kairosdb internal info metrics
        if (metric.contains("kairosdb.")) {
          continue;
        }
        client.deleteMetric(metric);
      }
    } catch (Exception e) {
      LOGGER.error("Delete old data failed because ", e);
    }
  }

  @Override
  public void close() throws TsdbException {
    try {
      client.close();
    } catch (IOException | NullPointerException e) {
      throw new TsdbException("Close KairosDB client failed, because " + e.getMessage());
    }
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) {
    // no need for KairosDB
    return 0.0;
  }

  private LinkedList<KairosDataModel> createDataModel(
      DeviceSchema deviceSchema, long timestamp, List<Object> recordValues) {
    LinkedList<KairosDataModel> models = new LinkedList<>();
    String groupId = deviceSchema.getGroup();
    int i = 0;
    for (Sensor sensor : deviceSchema.getSensors()) {
      KairosDataModel model = new KairosDataModel();
      model.setName(sensor.getName());
      // TODO: KairosDB do not support float as data sensorType, use double instead.
      model.setTimestamp(timestamp);
      model.setValue(recordValues.get(i));
      Map<String, String> tags = new HashMap<>();
      tags.put(GROUP_STR, groupId);
      tags.put(DEVICE_STR, deviceSchema.getDevice());
      model.setTags(tags);
      models.addLast(model);
      i++;
    }
    return models;
  }

  private LinkedList<KairosDataModel> createDataModel(
      DeviceSchema deviceSchema, long timestamp, List<Object> recordValues, int colIndex) {
    LinkedList<KairosDataModel> models = new LinkedList<>();
    String groupId = deviceSchema.getGroup();
    KairosDataModel model = new KairosDataModel();
    model.setName(deviceSchema.getSensors().get(colIndex).getName());
    // TODO: KairosDB do not support float as data sensorType, use double instead.
    model.setTimestamp(timestamp);
    model.setValue(recordValues.get(0));
    Map<String, String> tags = new HashMap<>();
    tags.put(GROUP_STR, groupId);
    tags.put(DEVICE_STR, deviceSchema.getDevice());
    model.setTags(tags);
    models.addLast(model);
    return models;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    LinkedList<KairosDataModel> models = new LinkedList<>();
    for (Record record : batch.getRecords()) {
      models.addAll(
          createDataModel(
              batch.getDeviceSchema(), record.getTimestamp(), record.getRecordDataValue()));
    }
    String body = JSON.toJSONString(models);
    LOGGER.debug("body: {}", body);
    try {

      String response = HttpRequest.sendPost(writeUrl, body);

      LOGGER.debug("response: {}", response);
      return new Status(true);
    } catch (Exception e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    long time = preciseQuery.getTimestamp();
    QueryBuilder builder = constructBuilder(time, time, preciseQuery.getDeviceSchema());
    return executeOneQuery(builder);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    long startTime = rangeQuery.getStartTimestamp();
    long endTime = rangeQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, rangeQuery.getDeviceSchema());
    return executeOneQuery(builder);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    long startTime = valueRangeQuery.getStartTimestamp();
    long endTime = valueRangeQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, valueRangeQuery.getDeviceSchema());
    Aggregator filterAggre =
        AggregatorFactory.createFilterAggregator(
            FilterOperation.LTE, valueRangeQuery.getValueThreshold());
    addAggreForQuery(builder, filterAggre);
    return executeOneQuery(builder);
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    long startTime = aggRangeQuery.getStartTimestamp();
    long endTime = aggRangeQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, aggRangeQuery.getDeviceSchema());
    // convert to second
    int timeInterval = (int) (endTime - startTime) + 1;
    Aggregator aggregator =
        new SamplingAggregator(aggRangeQuery.getAggFun(), timeInterval, TimeUnit.MILLISECONDS);
    addAggreForQuery(builder, aggregator);
    return executeOneQuery(builder);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    long startTime = aggValueQuery.getStartTimestamp();
    long endTime = aggValueQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, aggValueQuery.getDeviceSchema());
    Aggregator funAggre = new SamplingAggregator(aggValueQuery.getAggFun(), 5000, TimeUnit.YEARS);
    Aggregator filterAggre =
        AggregatorFactory.createFilterAggregator(
            FilterOperation.LTE, aggValueQuery.getValueThreshold());
    addAggreForQuery(builder, filterAggre, funAggre);
    return executeOneQuery(builder);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    long startTime = aggRangeValueQuery.getStartTimestamp();
    long endTime = aggRangeValueQuery.getEndTimestamp();
    QueryBuilder builder =
        constructBuilder(startTime, endTime, aggRangeValueQuery.getDeviceSchema());
    int timeInterval = (int) (endTime - startTime) + 1;
    Aggregator funAggre =
        new SamplingAggregator(aggRangeValueQuery.getAggFun(), timeInterval, TimeUnit.SECONDS);
    Aggregator filterAggre =
        AggregatorFactory.createFilterAggregator(
            FilterOperation.LTE, aggRangeValueQuery.getValueThreshold());
    addAggreForQuery(builder, filterAggre, funAggre);
    return executeOneQuery(builder);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    long startTime = groupByQuery.getStartTimestamp();
    long endTime = groupByQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, groupByQuery.getDeviceSchema());
    Aggregator funAggre =
        new SamplingAggregator(
            groupByQuery.getAggFun(), (int) groupByQuery.getGranularity(), TimeUnit.MILLISECONDS);
    addAggreForQuery(builder, funAggre);
    return executeOneQuery(builder);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    // latestPointQuery
    long startTime = latestPointQuery.getStartTimestamp();
    long endTime = latestPointQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, latestPointQuery.getDeviceSchema());
    Aggregator aggregator = AggregatorFactory.createLastAggregator(5000, TimeUnit.YEARS);
    addAggreForQuery(builder, aggregator);
    return executeOneQuery(builder);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    long startTime = rangeQuery.getStartTimestamp();
    long endTime = rangeQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, rangeQuery.getDeviceSchema());
    builder.getMetrics().get(0).setOrder(QueryMetric.Order.DESCENDING);
    return executeOneQuery(builder);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    long startTime = valueRangeQuery.getStartTimestamp();
    long endTime = valueRangeQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, valueRangeQuery.getDeviceSchema());
    Aggregator filterAggre =
        AggregatorFactory.createFilterAggregator(
            FilterOperation.LTE, valueRangeQuery.getValueThreshold());
    addAggreForQuery(builder, filterAggre);
    builder.getMetrics().get(0).setOrder(QueryMetric.Order.DESCENDING);
    return executeOneQuery(builder);
  }

  private Status executeOneQuery(QueryBuilder builder) {
    LOGGER.debug("[JSON] {}", builder);
    int queryResultPointNum = 0;
    try {
      QueryResponse response = client.query(builder);
      for (QueryResult query : response.getQueries()) {
        for (Result result : query.getResults()) {
          queryResultPointNum += result.getDataPoints().size();
        }
      }
      return new Status(true, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, 0, e, builder.toString());
    }
  }

  private QueryBuilder constructBuilder(long st, long et, List<DeviceSchema> deviceSchemaList) {
    QueryBuilder builder = QueryBuilder.getInstance();
    builder.setStart(new Date(st)).setEnd(new Date(et));
    for (DeviceSchema deviceSchema : deviceSchemaList) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        builder
            .addMetric(sensor.getName())
            .addTag(DEVICE_STR, deviceSchema.getDevice())
            .addTag(GROUP_STR, deviceSchema.getGroup());
      }
    }
    return builder;
  }

  private void addAggreForQuery(QueryBuilder builder, Aggregator... aggregatorArray) {
    builder
        .getMetrics()
        .forEach(
            queryMetric -> {
              for (Aggregator aggregator : aggregatorArray) {
                queryMetric.addAggregator(aggregator);
              }
            });
  }
}
