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

package cn.edu.tsinghua.iot.benchmark.tsdb.self;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelfCheck implements IDatabase {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(SelfCheck.class);
  private final Map<String, Long> deviceNameToMaxTime = new HashMap<>();
  private final Map<String, Long> deviceNameToTotalPoints = new HashMap<>();
  private final Map<String, Long> deviceNameToOutOfOrderPoints = new HashMap<>();

  public SelfCheck(DBConfig dbConfig) {
    // empty constructor
  }

  @Override
  public void init() throws TsdbException {}

  @Override
  public void cleanup() throws TsdbException {}

  @Override
  public void close() throws TsdbException {}

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    return 0.0;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    String deviceName = batch.getDeviceSchema().getDevice();
    long maxTime = deviceNameToMaxTime.getOrDefault(deviceName, 0L);
    long totalPoint = deviceNameToTotalPoints.getOrDefault(deviceName, 0L);
    long outOfOrderPoint = deviceNameToOutOfOrderPoints.getOrDefault(deviceName, 0L);
    for (Record record : batch.getRecords()) {
      long point = record.getRecordDataValue().size();
      totalPoint += point;
      if (record.getTimestamp() >= maxTime) {
        // in order
        maxTime = record.getTimestamp();
      } else {
        // out of order
        outOfOrderPoint += record.getRecordDataValue().size();
      }
    }
    deviceNameToMaxTime.put(deviceName, maxTime);
    deviceNameToTotalPoints.put(deviceName, totalPoint);
    deviceNameToOutOfOrderPoints.put(deviceName, outOfOrderPoint);
    if (totalPoint
        == config.getLOOP() * config.getBATCH_SIZE_PER_WRITE() * config.getSENSOR_NUMBER()) {
      logger.info(
          "Device: {}, total point: {}, out of order point: {}, out of order point ratio: {}",
          deviceName,
          totalPoint,
          outOfOrderPoint,
          outOfOrderPoint * 1.0 / totalPoint);
    }
    return new Status(true);
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return new Status(true, 0);
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return null;
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return null;
  }
}
