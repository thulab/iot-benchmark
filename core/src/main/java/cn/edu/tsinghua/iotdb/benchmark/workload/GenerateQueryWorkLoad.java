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

import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.enums.DBSwitch;
import cn.edu.tsinghua.iotdb.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class GenerateQueryWorkLoad extends QueryWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateQueryWorkLoad.class);

  private static final Random queryDeviceRandom = new Random(config.getQUERY_SEED());
  private static final Random querySensorRandom = new Random(config.getQUERY_SEED());
  private static final long timeStampConst =
      TimeUtils.getTimestampConst(config.getTIMESTAMP_PRECISION());
  private static AtomicInteger nowDeviceId = new AtomicInteger(config.getFIRST_DEVICE_INDEX());
  private Long currentTimestamp = null;

  private static final Map<Operation, Long> operationLoops = new EnumMap<>(Operation.class);;

  public GenerateQueryWorkLoad() {
    for (Operation operation : Operation.values()) {
      operationLoops.put(operation, 0L);
    }
  }

  @Override
  public PreciseQuery getPreciseQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(true);
    long timestamp = getQueryStartTimestamp(Operation.PRECISE_QUERY);
    return new PreciseQuery(queryDevices, timestamp);
  }

  @Override
  public RangeQuery getRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(true);
    long startTimestamp = getQueryStartTimestamp(Operation.RANGE_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new RangeQuery(queryDevices, startTimestamp, endTimestamp);
  }

  @Override
  public ValueRangeQuery getValueRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(false);
    long startTimestamp = getQueryStartTimestamp(Operation.VALUE_RANGE_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new ValueRangeQuery(
        queryDevices, startTimestamp, endTimestamp, config.getQUERY_LOWER_VALUE());
  }

  @Override
  public AggRangeQuery getAggRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices =
        getQueryDeviceSchemaList(config.getQUERY_AGGREGATE_FUN().startsWith("count"));
    long startTimestamp = getQueryStartTimestamp(Operation.AGG_RANGE_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new AggRangeQuery(
        queryDevices, startTimestamp, endTimestamp, config.getQUERY_AGGREGATE_FUN());
  }

  @Override
  public AggValueQuery getAggValueQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(false);
    return new AggValueQuery(
        queryDevices, config.getQUERY_AGGREGATE_FUN(), config.getQUERY_LOWER_VALUE());
  }

  @Override
  public AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(false);
    long startTimestamp = getQueryStartTimestamp(Operation.AGG_RANGE_VALUE_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new AggRangeValueQuery(
        queryDevices,
        startTimestamp,
        endTimestamp,
        config.getQUERY_AGGREGATE_FUN(),
        config.getQUERY_LOWER_VALUE());
  }

  @Override
  public GroupByQuery getGroupByQuery() throws WorkloadException {
    boolean typeAllow = false;
    if (config.getQUERY_AGGREGATE_FUN().contains("count")) {
      typeAllow = true;
    }
    if (config.getDbConfig().getDB_SWITCH() == DBSwitch.DB_INFLUX_2
        || (config.isIS_DOUBLE_WRITE()
            && config.getANOTHER_DBConfig().getDB_SWITCH() == DBSwitch.DB_INFLUX_2)) {
      typeAllow = false;
    }
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(typeAllow);
    long startTimestamp = getQueryStartTimestamp(Operation.GROUP_BY_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new GroupByQuery(
        queryDevices,
        startTimestamp,
        endTimestamp,
        config.getQUERY_AGGREGATE_FUN(),
        config.getGROUP_BY_TIME_UNIT());
  }

  @Override
  public LatestPointQuery getLatestPointQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(true);
    long startTimestamp = getQueryStartTimestamp(Operation.LATEST_POINT_QUERY);
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new LatestPointQuery(
        queryDevices, startTimestamp, endTimestamp, config.getQUERY_AGGREGATE_FUN());
  }

  @Override
  public VerificationQuery getVerifiedQuery(Batch batch) throws WorkloadException {
    return new VerificationQuery(batch);
  }

  @Override
  public DeviceQuery getDeviceQuery() {
    Integer deviceId = nowDeviceId.getAndIncrement();
    if (deviceId >= config.getFIRST_DEVICE_INDEX() + config.getDEVICE_NUMBER()) {
      return null;
    }
    DeviceSchema deviceSchema =
        new DeviceSchema(deviceId, config.getSENSORS(), config.getDEVICE_TAGS());
    return new DeviceQuery(deviceSchema);
  }

  @Override
  public void updateTime(long currentTimestamp) {
    this.currentTimestamp = currentTimestamp;
  }

  private long getQueryStartTimestamp(Operation operation) {
    if (currentTimestamp != null) {
      if (operation == Operation.PRECISE_QUERY) {
        return currentTimestamp;
      } else {
        return currentTimestamp >= config.getQUERY_INTERVAL()
            ? currentTimestamp - config.getQUERY_INTERVAL()
            : 0;
      }
    }
    long currentQueryLoop = operationLoops.get(operation);
    long timestampOffset = currentQueryLoop * config.getSTEP_SIZE() * config.getPOINT_STEP();
    operationLoops.put(operation, currentQueryLoop + 1);
    return Constants.START_TIMESTAMP * timeStampConst + timestampOffset;
  }

  /**
   * Return the list of deviceSchema
   *
   * @param typeAllow true: allow generating bool and text type.
   */
  private List<DeviceSchema> getQueryDeviceSchemaList(boolean typeAllow) throws WorkloadException {
    checkQuerySchemaParams();
    List<DeviceSchema> queryDevices = new ArrayList<>();
    List<Integer> queryDeviceIds = new ArrayList<>();
    List<Sensor> sensors = config.getSENSORS();
    while (queryDevices.size() < Math.min(config.getDEVICE_NUMBER(), config.getQUERY_DEVICE_NUM())
        && queryDeviceIds.size() < config.getDEVICE_NUMBER()) {
      // get a device belong to [first_device_index, first_device_index + device_number)
      int deviceId =
          queryDeviceRandom.nextInt(config.getDEVICE_NUMBER()) + config.getFIRST_DEVICE_INDEX();
      // avoid duplicate
      if (!queryDeviceIds.contains(deviceId)) {
        queryDeviceIds.add(deviceId);
      } else {
        continue;
      }
      List<Sensor> querySensors = new ArrayList<>();
      List<Integer> querySensorIds = new ArrayList<>();
      while (querySensors.size() < Math.min(config.getSENSOR_NUMBER(), config.getQUERY_SENSOR_NUM())
          && querySensorIds.size() < config.getSENSOR_NUMBER()) {
        // getSensor belong to [0, sensor_number)
        int sensorId = querySensorRandom.nextInt(config.getSENSOR_NUMBER());
        // avoid duplicate
        if (!querySensorIds.contains(sensorId)) {
          querySensorIds.add(sensorId);
        } else {
          continue;
        }
        Sensor sensor = sensors.get(sensorId);
        if (!typeAllow) {
          SensorType sensorType = sensor.getSensorType();
          if (sensorType == SensorType.BOOLEAN || sensorType == SensorType.TEXT) {
            continue;
          }
        }
        querySensors.add(sensor);
      }
      if (querySensors.size() != config.getQUERY_SENSOR_NUM()) {
        continue;
      }
      DeviceSchema deviceSchema = new DeviceSchema(deviceId, querySensors, config.getDEVICE_TAGS());
      queryDevices.add(deviceSchema);
    }
    if (queryDevices.size() != config.getQUERY_DEVICE_NUM()) {
      LOGGER.warn("There is no suitable sensor for query, please check INSERT_DATATYPE_PROPORTION");
      throw new WorkloadException(
          "There is no suitable sensor for query, please check INSERT_DATATYPE_PROPORTION");
    }
    return queryDevices;
  }

  private void checkQuerySchemaParams() throws WorkloadException {
    if (!(config.getQUERY_DEVICE_NUM() > 0
        && config.getQUERY_DEVICE_NUM() <= config.getDEVICE_NUMBER())) {
      throw new WorkloadException("getQUERY_DEVICE_NUM() is not correct, please check.");
    }
    if (!(config.getQUERY_SENSOR_NUM() > 0
        && config.getQUERY_SENSOR_NUM() <= config.getSENSOR_NUMBER())) {
      throw new WorkloadException("QUERY_SENSOR_NUM is not correct, please check.");
    }
  }
}
