package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SyntheticWorkload implements IWorkload {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static Random timestampRandom = new Random(config.DATA_SEED);
  private ProbTool probTool;
  private long maxTimestampIndex;
  private Random poissonRandom;
  private Random queryDeviceRandom;
  private Map<Operation, Long> operationLoops;
  private static Random random = new Random();
  private static final String DECIMAL_FORMAT = "%." + config.NUMBER_OF_DECIMAL_DIGIT + "f";

  public SyntheticWorkload(int clientId) {
    probTool = new ProbTool();
    maxTimestampIndex = 0;
    poissonRandom = new Random(config.DATA_SEED);
    queryDeviceRandom = new Random(config.QUERY_SEED + clientId);
    operationLoops = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLoops.put(operation, 0L);
    }
  }

  private static long getCurrentTimestamp(long stepOffset) {
    long timeStampOffset = config.POINT_STEP * stepOffset;
    if (config.IS_OVERFLOW) {
      timeStampOffset += random.nextDouble() * config.POINT_STEP;
    }
    long currentTimestamp = Constants.START_TIMESTAMP + timeStampOffset;
    if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
      currentTimestamp += (long) (config.POINT_STEP * timestampRandom.nextDouble());
    }
    return currentTimestamp;
  }

  private Batch getOrderedBatch(DeviceSchema deviceSchema, long loopIndex) {
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
      long stepOffset = loopIndex * config.BATCH_SIZE + batchOffset;
      addOneRowIntoBatch(deviceSchema, batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  private Batch getLocalOutOfOrderBatch(DeviceSchema deviceSchema, long loopIndex) {
      Batch batch = new Batch();
      long barrier = (long) (config.BATCH_SIZE * config.OVERFLOW_RATIO);
      long stepOffset = loopIndex * config.BATCH_SIZE + barrier;
      addOneRowIntoBatch(deviceSchema, batch, stepOffset);
      for (long batchOffset = 0; batchOffset < barrier; batchOffset++) {
          stepOffset = loopIndex * config.BATCH_SIZE + batchOffset;
          addOneRowIntoBatch(deviceSchema, batch, stepOffset);
      }
      for (long batchOffset = barrier + 1; batchOffset < config.BATCH_SIZE; batchOffset++) {
          stepOffset = loopIndex * config.BATCH_SIZE + batchOffset;
          addOneRowIntoBatch(deviceSchema, batch, stepOffset);
      }
      batch.setDeviceSchema(deviceSchema);
      return batch;
  }

  private Batch getDistOutOfOrderBatch(DeviceSchema deviceSchema) {
    Batch batch = new Batch();
    PossionDistribution possionDistribution = new PossionDistribution(poissonRandom);
    int nextDelta;
    long stepOffset;
    for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
      if (probTool.returnTrueByProb(config.OVERFLOW_RATIO, poissonRandom)) {
        // generate overflow timestamp
        nextDelta = possionDistribution.getNextPossionDelta();
        stepOffset = maxTimestampIndex - nextDelta;
      } else {
        // generate normal increasing timestamp
        maxTimestampIndex++;
        stepOffset = maxTimestampIndex;
      }
      addOneRowIntoBatch(deviceSchema, batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  static void addOneRowIntoBatch(DeviceSchema deviceSchema, Batch batch, long stepOffset) {
    List<String> values = new ArrayList<>();
    long currentTimestamp;
    currentTimestamp = getCurrentTimestamp(stepOffset);
    for (String sensor : deviceSchema.getSensors()) {
      FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
      String value = String.format(DECIMAL_FORMAT,
          Function.getValueByFuntionidAndParam(param, currentTimestamp).floatValue());
      values.add(value);
    }
    batch.add(currentTimestamp, values);
  }

  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException {
    if (!config.IS_OVERFLOW) {
      return getOrderedBatch(deviceSchema, loopIndex);
    } else {
      switch (config.OVERFLOW_MODE) {
        case 0:
          return getDistOutOfOrderBatch(deviceSchema);
        case 1:
          return getLocalOutOfOrderBatch(deviceSchema, loopIndex);
        default:
          throw new WorkloadException("Unsupported overflow mode: " + config.OVERFLOW_MODE);
      }
    }
  }

  private List<DeviceSchema> getQueryDeviceSchemaList() throws WorkloadException {
    checkQuerySchemaParams();
    List<DeviceSchema> queryDevices = new ArrayList<>();
    List<Integer> clientDevicesIndex = new ArrayList<>();
    for (int m = 0; m < config.DEVICE_NUMBER; m++) {
      clientDevicesIndex.add(m);
    }
    Collections.shuffle(clientDevicesIndex, queryDeviceRandom);
    for (int m = 0; m < config.QUERY_DEVICE_NUM; m++) {
      DeviceSchema deviceSchema = new DeviceSchema(clientDevicesIndex.get(m));
      List<String> sensors = deviceSchema.getSensors();
      Collections.shuffle(sensors, queryDeviceRandom);
      List<String> querySensors = new ArrayList<>();
      for (int i = 0; i < config.QUERY_SENSOR_NUM; i++) {
        querySensors.add(sensors.get(i));
      }
      deviceSchema.setSensors(querySensors);
      queryDevices.add(deviceSchema);
    }
    return queryDevices;
  }

  private void checkQuerySchemaParams() throws WorkloadException {
    if (!(config.QUERY_DEVICE_NUM > 0 && config.QUERY_DEVICE_NUM <= config.DEVICE_NUMBER)) {
      throw new WorkloadException("QUERY_DEVICE_NUM is not correct, please check.");
    }
    if (!(config.QUERY_SENSOR_NUM > 0 && config.QUERY_SENSOR_NUM <= config.SENSOR_NUMBER)) {
      throw new WorkloadException("QUERY_SENSOR_NUM is not correct, please check.");
    }
  }

  private long getQueryStartTimestamp() {
    long currentQueryLoop = operationLoops.get(Operation.PRECISE_QUERY);
    long timestampOffset = currentQueryLoop * config.STEP_SIZE * config.POINT_STEP;
    operationLoops.put(Operation.PRECISE_QUERY, currentQueryLoop + 1);
    return Constants.START_TIMESTAMP + timestampOffset;
  }

  public PreciseQuery getPreciseQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long timestamp = getQueryStartTimestamp();
    return new PreciseQuery(queryDevices, timestamp);
  }

  public RangeQuery getRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new RangeQuery(queryDevices, startTimestamp, endTimestamp);
  }

  public ValueRangeQuery getValueRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new ValueRangeQuery(queryDevices, startTimestamp, endTimestamp,
        config.QUERY_LOWER_LIMIT);
  }

  public AggRangeQuery getAggRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new AggRangeQuery(queryDevices, startTimestamp, endTimestamp,
        config.QUERY_AGGREGATE_FUN);
  }

  public AggValueQuery getAggValueQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    return new AggValueQuery(queryDevices, config.QUERY_AGGREGATE_FUN, config.QUERY_LOWER_LIMIT);
  }

  public AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new AggRangeValueQuery(queryDevices, startTimestamp, endTimestamp,
        config.QUERY_AGGREGATE_FUN, config.QUERY_LOWER_LIMIT);
  }

  public GroupByQuery getGroupByQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new GroupByQuery(queryDevices, startTimestamp, endTimestamp, config.QUERY_AGGREGATE_FUN,
        config.TIME_UNIT);
  }

  public LatestPointQuery getLatestPointQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new LatestPointQuery(queryDevices, startTimestamp, endTimestamp,
        config.QUERY_AGGREGATE_FUN);
  }


}
