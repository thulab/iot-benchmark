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

package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.GeolifeReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.NOAAReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.ReddReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.TDriveReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.ArrayList;
import java.util.List;

public class RealDatasetWorkLoad implements IGenerateWorkload {

  private BasicReader reader;

  private Config config;
  private List<DeviceSchema> deviceSchemaList;
  private long startTime;
  private long endTime;

  /**
   * Init reader of real dataset write test
   *
   * @param files real dataset files
   * @param config config
   */
  public RealDatasetWorkLoad(List<String> files, Config config) {
    switch (config.getDATA_SET()) {
      case TDRIVE:
        reader = new TDriveReader(files);
        break;
      case REDD:
        reader = new ReddReader(files);
        break;
      case GEOLIFE:
        reader = new GeolifeReader(files);
        break;
      case NOAA:
        reader = new NOAAReader(files);
        break;
      default:
        throw new RuntimeException(config.getDATA_SET() + " not supported");
    }
  }

  /**
   * read test.
   *
   * @param config config
   */
  public RealDatasetWorkLoad(Config config) {
    this.config = config;

    // init sensor list
    List<String> sensorList = new ArrayList<>();
    for (int i = 0; i < config.getQUERY_SENSOR_NUM(); i++) {
      sensorList.add(config.getFIELDS().get(i));
    }

    // init device schema list
    deviceSchemaList = new ArrayList<>();
    for (int i = 1; i <= config.getQUERY_DEVICE_NUM(); i++) {
      String deviceIdStr = "" + i;
      DeviceSchema deviceSchema =
          new DeviceSchema(
              calGroupIdStr(deviceIdStr, config.getGROUP_NUMBER()), deviceIdStr, sensorList);
      deviceSchemaList.add(deviceSchema);
    }

    // init startTime, endTime
    startTime = config.getREAL_DATASET_QUERY_START_TIME();
    endTime = config.getREAL_DATASET_QUERY_STOP_TIME();
  }

  public Batch getOneBatch() {
    if (reader.hasNextBatch()) {
      return reader.nextBatch();
    } else {
      return null;
    }
  }

  @Override
  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException {
    throw new WorkloadException("not support in real data workload.");
  }

  @Override
  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex, int colIndex)
      throws WorkloadException {
    throw new WorkloadException("not support in real data workload.");
  }

  @Override
  public PreciseQuery getPreciseQuery() {
    return new PreciseQuery(deviceSchemaList, startTime);
  }

  @Override
  public RangeQuery getRangeQuery() {
    return new RangeQuery(deviceSchemaList, startTime, endTime);
  }

  @Override
  public ValueRangeQuery getValueRangeQuery() {
    return new ValueRangeQuery(deviceSchemaList, startTime, endTime, config.getQUERY_LOWER_VALUE());
  }

  @Override
  public AggRangeQuery getAggRangeQuery() {
    return new AggRangeQuery(deviceSchemaList, startTime, endTime, config.getQUERY_AGGREGATE_FUN());
  }

  @Override
  public AggValueQuery getAggValueQuery() {
    return new AggValueQuery(
        deviceSchemaList,
        startTime,
        endTime,
        config.getQUERY_AGGREGATE_FUN(),
        config.getQUERY_LOWER_VALUE());
  }

  @Override
  public AggRangeValueQuery getAggRangeValueQuery() {
    return new AggRangeValueQuery(
        deviceSchemaList,
        startTime,
        endTime,
        config.getQUERY_AGGREGATE_FUN(),
        config.getQUERY_LOWER_VALUE());
  }

  @Override
  public GroupByQuery getGroupByQuery() {
    return new GroupByQuery(
        deviceSchemaList,
        startTime,
        endTime,
        config.getQUERY_AGGREGATE_FUN(),
        config.getQUERY_INTERVAL());
  }

  @Override
  public LatestPointQuery getLatestPointQuery() {
    return new LatestPointQuery(deviceSchemaList, startTime, endTime, "last");
  }

  static String calGroupIdStr(String deviceId, int groupNum) {
    return String.valueOf(deviceId.hashCode() % groupNum);
  }
}
