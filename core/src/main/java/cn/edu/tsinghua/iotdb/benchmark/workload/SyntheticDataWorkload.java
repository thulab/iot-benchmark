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
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PoissonDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.schema.enums.Type;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IGenerateDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SyntheticDataWorkload implements IGenerateDataWorkload {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyntheticDataWorkload.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final Random timestampRandom = new Random(config.getDATA_SEED());

  private static final ProbTool probTool = new ProbTool();
  private static final Random poissonRandom = new Random(config.getDATA_SEED());

  private final Map<DeviceSchema, Long> maxTimestampIndexMap;
  private final Map<Operation, Long> operationLoops;

  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

  private final Random queryDeviceRandom;
  private static final Random random = new Random(config.getDATA_SEED());
  private static final Random dataRandom = new Random(config.getDATA_SEED());

  /**
   * workloadValues[SENSOR_NUMBER][WORKLOAD_BUFFER_SIZE]。 For those regular data, a piece of data of
   * each sensor is stored for rapid generation according to the law
   */
  private static final Object[][] workloadValues = initWorkloadValues();

  private static final String CHAR_TABLE =
      "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final long timeStampConst = getTimestampConst(config.getTIMESTAMP_PRECISION());

  public SyntheticDataWorkload(int clientId) {
    maxTimestampIndexMap = new HashMap<>();
    for (DeviceSchema schema : BaseDataSchema.getInstance().getThreadDeviceSchema(clientId)) {
      maxTimestampIndexMap.put(schema, 0L);
    }
    queryDeviceRandom = new Random(config.getQUERY_SEED() + clientId);
    operationLoops = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLoops.put(operation, 0L);
    }
  }

  /**
   * Init workload values
   *
   * @return
   */
  private static Object[][] initWorkloadValues() {
    Object[][] workloadValues = null;
    if (!config.getOPERATION_PROPORTION().split(":")[0].equals("0")) {
      // if the first number in OPERATION_PROPORTION not equals to 0, then write data
      workloadValues = new Object[config.getSENSOR_NUMBER()][config.getWORKLOAD_BUFFER_SIZE()];
      for (int j = 0; j < config.getSENSOR_NUMBER(); j++) {
        String sensor = config.getSENSOR_CODES().get(j);
        Type sensorType =
            baseDataSchema.getSensorType(
                MetaUtil.getDeviceName(config.getFIRST_DEVICE_INDEX()), sensor);
        for (int i = 0; i < config.getWORKLOAD_BUFFER_SIZE(); i++) {
          // This time stamp is only used to generate periodic data. So the timestamp is also
          // periodic
          long currentTimestamp = getCurrentTimestamp(i);
          Object value;
          if (sensorType == Type.TEXT) {
            // TEXT case: pick STRING_LENGTH chars to be a String for insertion.
            StringBuffer builder = new StringBuffer(config.getSTRING_LENGTH());
            for (int k = 0; k < config.getSTRING_LENGTH(); k++) {
              builder.append(CHAR_TABLE.charAt(dataRandom.nextInt(CHAR_TABLE.length())));
            }
            value = builder.toString();
          } else {
            // not TEXT case
            FunctionParam param = config.getSENSOR_FUNCTION().get(sensor);
            Number number = Function.getValueByFunctionIdAndParam(param, currentTimestamp);
            switch (sensorType) {
              case BOOLEAN:
                value = number.floatValue() > ((param.getMax() + param.getMin()) / 2);
                break;
              case INT32:
                value = number.intValue();
                break;
              case INT64:
                value = number.longValue();
                break;
              case FLOAT:
                value = (float) (Math.round(number.floatValue()));
                break;
              case DOUBLE:
                value = (double) Math.round(number.doubleValue());
                break;
              default:
                value = null;
                break;
            }
          }
          workloadValues[j][i] = value;
        }
      }
    } else {
      LOGGER.info("According to OPERATION_PROPORTION, there is no need to write");
    }
    return workloadValues;
  }

  @Override
  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException {
    if (!config.isIS_OUT_OF_ORDER()) {
      return getOrderedBatch(deviceSchema, loopIndex);
    } else {
      switch (config.getOUT_OF_ORDER_MODE()) {
        case 0:
          return getDistOutOfOrderBatch(deviceSchema);
        case 1:
          return getLocalOutOfOrderBatch(deviceSchema, loopIndex);
        default:
          throw new WorkloadException(
              "Unsupported out of order mode: " + config.getOUT_OF_ORDER_MODE());
      }
    }
  }

  @Override
  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex, int colIndex)
      throws WorkloadException {
    if (!config.isIS_OUT_OF_ORDER()) {
      return getOrderedBatch(deviceSchema, loopIndex, colIndex);
    } else {
      switch (config.getOUT_OF_ORDER_MODE()) {
        case 0:
          return getDistOutOfOrderBatch(deviceSchema);
        case 1:
          return getLocalOutOfOrderBatch(deviceSchema, loopIndex);
        default:
          throw new WorkloadException(
              "Unsupported out of order mode: " + config.getOUT_OF_ORDER_MODE());
      }
    }
  }

  /**
   * Get timestamp according to stepOffset
   *
   * @param stepOffset
   * @return
   */
  private static long getCurrentTimestamp(long stepOffset) {
    // offset of data ahead
    long offset = config.getPOINT_STEP() * stepOffset;
    // timestamp for next data
    long timestamp = 0;
    // change timestamp frequency
    if (config.isIS_REGULAR_FREQUENCY()) {
      // data is in regular frequency, then do nothing
      timestamp += config.getPOINT_STEP();
    } else {
      // data is not in regular frequency, then use random
      timestamp += config.getPOINT_STEP() * timestampRandom.nextDouble();
    }
    return (Constants.START_TIMESTAMP + offset + timestamp) * timeStampConst;
  }

  /**
   * Generate batch in order, each row contains data from all sensors
   *
   * @param deviceSchema
   * @param loopIndex
   * @return
   */
  private Batch getOrderedBatch(DeviceSchema deviceSchema, long loopIndex) {
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      long stepOffset = loopIndex * config.getBATCH_SIZE_PER_WRITE() + batchOffset;
      addOneRowIntoBatch(batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  /**
   * Generate batch in order, each row contains data from sensor which index is colIndex
   *
   * @param deviceSchema
   * @param loopIndex
   * @param colIndex
   * @return
   */
  private Batch getOrderedBatch(DeviceSchema deviceSchema, long loopIndex, int colIndex) {
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      long stepOffset = loopIndex * config.getBATCH_SIZE_PER_WRITE() + batchOffset;
      addOneRowIntoBatch(batch, stepOffset, colIndex);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  /**
   * Generate out of order batch in mode 0
   *
   * @param deviceSchema
   * @return
   */
  private Batch getDistOutOfOrderBatch(DeviceSchema deviceSchema) {
    Batch batch = new Batch();
    PoissonDistribution poissonDistribution = new PoissonDistribution(poissonRandom);
    int nextDelta;
    long stepOffset;
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      if (probTool.returnTrueByProb(config.getOUT_OF_ORDER_RATIO(), poissonRandom)) {
        // generate out of order timestamp
        nextDelta = poissonDistribution.getNextPoissonDelta();
        stepOffset = maxTimestampIndexMap.get(deviceSchema) - nextDelta;
      } else {
        // generate normal increasing timestamp
        maxTimestampIndexMap.put(deviceSchema, maxTimestampIndexMap.get(deviceSchema) + 1);
        stepOffset = maxTimestampIndexMap.get(deviceSchema);
      }
      addOneRowIntoBatch(batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  /**
   * Generate out of order batch in mode 1
   *
   * @param deviceSchema
   * @param loopIndex
   * @return
   */
  private Batch getLocalOutOfOrderBatch(DeviceSchema deviceSchema, long loopIndex) {
    Batch batch = new Batch();
    int moveOffset = config.getMAX_K() % config.getBATCH_SIZE_PER_WRITE();
    if (moveOffset == 0) {
      moveOffset = 1;
    }
    // like circular array
    int barrier = (int) (config.getBATCH_SIZE_PER_WRITE() * config.getOUT_OF_ORDER_RATIO());
    // out of order batch
    long targetBatch;
    if (loopIndex >= moveOffset) {
      targetBatch = loopIndex - moveOffset;
    } else {
      targetBatch = loopIndex - moveOffset + config.getLOOP();
    }
    if (targetBatch > config.getLOOP()) {
      LOGGER.warn("Error loop");
    }
    // add out of order data
    for (int i = barrier - 1; i >= 0; i--) {
      long offset = targetBatch * config.getBATCH_SIZE_PER_WRITE() + i;
      addOneRowIntoBatch(batch, offset);
    }
    // add in order data
    for (int i = barrier; i < config.getBATCH_SIZE_PER_WRITE(); i++) {
      long offset = loopIndex * config.getBATCH_SIZE_PER_WRITE() + i;
      addOneRowIntoBatch(batch, offset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  /**
   * Add one row into batch, row contains data from all sensors
   *
   * @param batch
   * @param stepOffset
   */
  static void addOneRowIntoBatch(Batch batch, long stepOffset) {
    List<Object> values = new ArrayList<>();
    long currentTimestamp = getCurrentTimestamp(stepOffset);
    for (int i = 0; i < config.getSENSOR_NUMBER(); i++) {
      values.add(
          workloadValues[i][(int) (Math.abs(stepOffset) % config.getWORKLOAD_BUFFER_SIZE())]);
    }
    batch.add(currentTimestamp, values);
  }

  /**
   * Add one row into batch, row contains data from one sensor which index is colIndex
   *
   * @param batch
   * @param stepOffset
   * @param colIndex
   */
  static void addOneRowIntoBatch(Batch batch, long stepOffset, int colIndex) {
    List<Object> values = new ArrayList<>();
    long currentTimestamp = getCurrentTimestamp(stepOffset);
    values.add(
        workloadValues[colIndex][(int) (Math.abs(stepOffset) % config.getWORKLOAD_BUFFER_SIZE())]);
    batch.add(currentTimestamp, values);
  }
  /**
   * 返回设备列表
   *
   * @param typeAllow true: 允许 boolean 和 text类型进入
   * @return
   * @throws WorkloadException
   */
  private List<DeviceSchema> getQueryDeviceSchemaList(boolean typeAllow) throws WorkloadException {
    checkQuerySchemaParams();
    List<DeviceSchema> queryDevices = new ArrayList<>();
    List<Integer> clientDevicesIndex = new ArrayList<>();
    for (int m = config.getFIRST_DEVICE_INDEX();
        m
            < config.getDEVICE_NUMBER() * config.getREAL_INSERT_RATE()
                + config.getFIRST_DEVICE_INDEX();
        m++) {
      clientDevicesIndex.add(m);
    }
    Collections.shuffle(clientDevicesIndex, queryDeviceRandom);
    for (int m = 0;
        queryDevices.size() < config.getQUERY_DEVICE_NUM() && m < clientDevicesIndex.size();
        m++) {
      int deviceId = clientDevicesIndex.get(m);
      String device = MetaUtil.getDeviceName(deviceId);
      List<String> sensors = config.getSENSOR_CODES();
      Collections.shuffle(sensors, queryDeviceRandom);
      List<String> querySensors = new ArrayList<>();
      for (int i = 0;
          querySensors.size() < config.getQUERY_SENSOR_NUM() && i < sensors.size();
          i++) {
        String sensor = sensors.get(i);
        if (!typeAllow) {
          Type type = baseDataSchema.getSensorType(device, sensor);
          if (type == Type.BOOLEAN || type == Type.TEXT) {
            continue;
          }
        }
        querySensors.add(sensor);
      }
      if (querySensors.size() != config.getQUERY_SENSOR_NUM()) {
        continue;
      }
      DeviceSchema deviceSchema = new DeviceSchema(deviceId, querySensors);
      queryDevices.add(deviceSchema);
    }
    if (queryDevices.size() == 0) {
      LOGGER.error("Not Suitable DeviceSchema");
      throw new WorkloadException("No Suitable DeviceSchema");
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

  private long getQueryStartTimestamp() {
    long currentQueryLoop = operationLoops.get(Operation.PRECISE_QUERY);
    long timestampOffset = currentQueryLoop * config.getSTEP_SIZE() * config.getPOINT_STEP();
    operationLoops.put(Operation.PRECISE_QUERY, currentQueryLoop + 1);
    return Constants.START_TIMESTAMP * timeStampConst + timestampOffset;
  }

  @Override
  public PreciseQuery getPreciseQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(true);
    long timestamp = getQueryStartTimestamp();
    return new PreciseQuery(queryDevices, timestamp);
  }

  @Override
  public RangeQuery getRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(true);
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new RangeQuery(queryDevices, startTimestamp, endTimestamp);
  }

  @Override
  public ValueRangeQuery getValueRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(false);
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new ValueRangeQuery(
        queryDevices, startTimestamp, endTimestamp, config.getQUERY_LOWER_VALUE());
  }

  @Override
  public AggRangeQuery getAggRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices =
        getQueryDeviceSchemaList(config.getQUERY_AGGREGATE_FUN().startsWith("count"));
    long startTimestamp = getQueryStartTimestamp();
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
    long startTimestamp = getQueryStartTimestamp();
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
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList(false);
    long startTimestamp = getQueryStartTimestamp();
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
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new LatestPointQuery(
        queryDevices, startTimestamp, endTimestamp, config.getQUERY_AGGREGATE_FUN());
  }

  private static long getTimestampConst(String timePrecision) {
    if (timePrecision.equals("ms")) {
      return 1L;
    } else if (timePrecision.equals("us")) {
      return 1000L;
    } else {
      return 1000000L;
    }
  }
}
