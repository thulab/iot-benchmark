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

package cn.edu.tsinghua.iot.benchmark.tsdb;

import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.DeviceQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.VerificationQuery;

import java.sql.SQLException;
import java.util.List;

public interface IDatabase {

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  void init() throws TsdbException;

  /**
   * Cleanup any state for this DB, including the old data deletion. Called once before each test if
   * IS_DELETE_DATA=true.
   */
  void cleanup() throws TsdbException;

  /** Close the DB instance connections. Called once per DB instance. */
  void close() throws TsdbException;

  /**
   * Called once before each test if CREATE_SCHEMA=true.
   *
   * @param schemaList schema of devices to register
   * @return register schema time in second, return null when failed
   */
  Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException;

  /**
   * Insert one batch into the database, the DB implementation needs to resolve the data in batch
   * which contains device schema and Map[Long, List[String]] records. The key of records is a
   * timestamp and the value is a list of sensor value data.
   *
   * @param batch universal insertion data structure
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status insertOneBatch(IBatch batch) throws DBConnectException;

  default Status insertOneBatchWithCheck(IBatch batch) throws Exception {
    batch.reset();
    Status status = insertOneBatch(batch);
    batch.finishCheck();
    return status;
  }

  /**
   * Query data of one or multiple sensors at a precise timestamp. e.g. select v1... from data where
   * time = ? and device in ?
   *
   * @param preciseQuery universal precise query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status preciseQuery(PreciseQuery preciseQuery);

  /**
   * Query data of one or multiple sensors in a time range. e.g. select v1... from data where time
   * >= ? and time <= ? and device in ?
   *
   * @param rangeQuery universal range query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status rangeQuery(RangeQuery rangeQuery);

  /**
   * Query data of one or multiple sensors in a time range with a value filter. e.g. select v1...
   * from data where time >= ? and time <= ? and v1 > ? and device in ?
   *
   * @param valueRangeQuery contains universal range query with value filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status valueRangeQuery(ValueRangeQuery valueRangeQuery);

  /**
   * Query aggregated data of one or multiple sensors in a time range using aggregation function.
   * e.g. select func(v1)... from data where device in ? and time >= ? and time <= ?
   *
   * @param aggRangeQuery contains universal aggregation query with time filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status aggRangeQuery(AggRangeQuery aggRangeQuery);

  /**
   * Query aggregated data of one or multiple sensors in the whole time range. e.g. select
   * func(v1)... from data where device in ? and value > ? if value's type not support >, then
   * should ignore
   *
   * @param aggValueQuery contains universal aggregation query with value filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status aggValueQuery(AggValueQuery aggValueQuery);

  /**
   * Query aggregated data of one or multiple sensors with both time and value filters. e.g. select
   * func(v1)... from data where device in ? and time >= ? and time <= ? and value > ? if value's
   * type not support >, then should ignore
   *
   * @param aggRangeValueQuery contains universal aggregation query with time and value filters
   *     parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery);

  /**
   * Query aggregated group-by-time data of one or multiple sensors within a time range. e.g. SELECT
   * max(s_0), max(s_1) FROM group_0, group_1 WHERE ( device = ’d_3’ OR device = ’d_8’) AND time >=
   * 2010-01-01 12:00:00 AND time <= 2010-01-01 12:10:00 GROUP BY time(60000ms)
   *
   * @param groupByQuery contains universal group by query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status groupByQuery(GroupByQuery groupByQuery);

  /**
   * Query the latest(max-timestamp) data of one or multiple sensors. e.g. select time, v1... where
   * device = ? and time = max(time)
   *
   * @param latestPointQuery contains universal latest point query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status latestPointQuery(LatestPointQuery latestPointQuery);

  /** similar to rangeQuery, but order by time desc. */
  Status rangeQueryOrderByDesc(RangeQuery rangeQuery);

  /** similar to rangeQuery, but order by time desc. */
  Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery);

  default Status groupByQueryOrderByDesc(GroupByQuery groupByQuery) {
    throw new UnsupportedOperationException("This operation is not supported for this database");
  }

  /** Using in verification */
  default Status verificationQuery(VerificationQuery verificationQuery) {
    WorkloadException workloadException = new WorkloadException("Not Supported Verification Query");
    return new Status(false, 0, workloadException, workloadException.getMessage());
  }

  /** Verification between two database */
  default Status deviceQuery(DeviceQuery deviceQuery) throws SQLException, TsdbException {
    WorkloadException workloadException = new WorkloadException("Not Supported Verification Query");
    return new Status(false, 0, workloadException, workloadException.getMessage());
  }

  /** get summary of device */
  default DeviceSummary deviceSummary(DeviceQuery deviceQuery) throws SQLException, TsdbException {
    throw new TsdbException("Not Supported get summary of device.");
  }

  /**
   * map the given type string name to the name in the target DB
   *
   * @param iotdbSensorType : "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"
   */
  default String typeMap(SensorType iotdbSensorType) {
    return iotdbSensorType.name;
  }
}
