package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PoissonDistribution;
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
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SyntheticWorkload implements IWorkload {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyntheticWorkload.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static Random timestampRandom = new Random(config.DATA_SEED);
  private ProbTool probTool;
  private Map<DeviceSchema, Long> maxTimestampIndexMap;
  private Random poissonRandom;
  private Random queryDeviceRandom;
  private Map<Operation, Long> operationLoops;
  private static Random random = new Random(config.DATA_SEED);
  private static final String DECIMAL_FORMAT = "%." + config.NUMBER_OF_DECIMAL_DIGIT + "f";
  private static Random dataRandom = new Random(config.DATA_SEED);
  private static String[][] workloadValues = initWorkloadValues();
  private static final String CHAR_TABLE =
      "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final long timeStampConst = getTimestampConst(config.TIMESTAMP_PRECISION);

  public SyntheticWorkload(int clientId) {
    probTool = new ProbTool();
    maxTimestampIndexMap = new HashMap<>();
    poissonRandom = new Random(config.DATA_SEED);
    for(DeviceSchema schema: DataSchema.getInstance().getClientBindSchema().get(clientId)) {
      maxTimestampIndexMap.put(schema, 0L);
    }
    queryDeviceRandom = new Random(config.QUERY_SEED + clientId);
    operationLoops = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLoops.put(operation, 0L);
    }
  }

  private static String[][] initWorkloadValues() {
    String[][] workloadValues = null;
    if(!config.OPERATION_PROPORTION.split(":")[0].equals("0")) {
      workloadValues = new String[config.SENSOR_NUMBER][config.WORKLOAD_BUFFER_SIZE];
      int sensorIndex = 0;
      for (int j = 0; j < config.SENSOR_NUMBER; j++) {
        String sensor = config.SENSOR_CODES.get(j);
        for (int i = 0; i < config.WORKLOAD_BUFFER_SIZE; i++) {
          long currentTimestamp = getCurrentTimestamp(i);
          String value;
          if (!getNextDataType(sensorIndex).equals("TEXT")) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            value = String.format(DECIMAL_FORMAT,
                Function.getValueByFuntionidAndParam(param, currentTimestamp).floatValue());
          } else {
            //TEXT case: pick NUMBER_OF_DECIMAL_DIGIT chars to be a String for insertion.
            StringBuilder builder = new StringBuilder();
            for (int k = 0; k < config.NUMBER_OF_DECIMAL_DIGIT; k++) {
              assert dataRandom != null;
              builder.append(CHAR_TABLE.charAt(dataRandom.nextInt(CHAR_TABLE.length())));
            }
            value = builder.toString();
          }
          workloadValues[j][i] = value;
        }
        sensorIndex++;
      }
    }
    return workloadValues;
  }

  public static String getNextDataType(int sensorIndex) {
    List<Double> proportion = resolveDataTypeProportion();
    double[] p = new double[TSDataType.values().length + 1];
    p[0] = 0.0;
    // split [0,1] to n regions, each region corresponds to a data type whose proportion
    // is the region range size.
    for (int i = 1; i <= TSDataType.values().length; i++) {
      p[i] = p[i - 1] + proportion.get(i - 1);
    }
    double sensorPosition = sensorIndex * 1.0 / config.SENSOR_NUMBER;
    int i;
    for (i = 1; i <= TSDataType.values().length; i++) {
      if (sensorPosition >= p[i - 1] && sensorPosition < p[i]) {
        break;
      }
    }
    switch (i) {
      case 1:
        return "BOOLEAN";
      case 2:
        return "INT32";
      case 3:
        return "INT64";
      case 4:
        return "FLOAT";
      case 5:
        return "DOUBLE";
      case 6:
        return "TEXT";
      default:
        LOGGER.error("Unsupported data type {}, use default data type: TEXT.", i);
        return "TEXT";
    }
  }

  public static List<Double> resolveDataTypeProportion() {
    List<Double> proportion = new ArrayList<>();
    String[] split = config.INSERT_DATATYPE_PROPORTION.split(":");
    if (split.length != TSDataType.values().length) {
      LOGGER.error("INSERT_DATATYPE_PROPORTION error, please check this parameter.");
    }
    double[] proportions = new double[TSDataType.values().length];
    double sum = 0;
    for (int i = 0; i < split.length; i++) {
      proportions[i] = Double.parseDouble(split[i]);
      sum += proportions[i];
    }
    for (int i = 0; i < split.length; i++) {
      if (sum != 0) {
        proportion.add(proportions[i] / sum);
      } else {
        proportion.add(0.0);
        LOGGER.error("The sum of INSERT_DATATYPE_PROPORTION is zero!");
      }
    }
    return proportion;
  }

  private static long getCurrentTimestamp(long stepOffset) {
    long timeStampOffset = config.POINT_STEP * stepOffset;
    if (config.IS_OVERFLOW) {
      timeStampOffset += (long) (random.nextDouble() * config.POINT_STEP);
    } else {
      if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
        timeStampOffset += (long) (config.POINT_STEP * timestampRandom.nextDouble());
      }
    }
    long currentTimestamp = Constants.START_TIMESTAMP * timeStampConst + timeStampOffset;
    return currentTimestamp;
  }

  private Batch getOrderedBatch(DeviceSchema deviceSchema, long loopIndex) {
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
      long stepOffset = loopIndex * config.BATCH_SIZE + batchOffset;
      addOneRowIntoBatch(batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  private Batch getLocalOutOfOrderBatch(DeviceSchema deviceSchema, long loopIndex) {
      Batch batch = new Batch();
      long barrier = (long) (config.BATCH_SIZE * config.OVERFLOW_RATIO);
      long stepOffset = loopIndex * config.BATCH_SIZE + barrier;
      addOneRowIntoBatch(batch, stepOffset);
      for (long batchOffset = 0; batchOffset < barrier; batchOffset++) {
          stepOffset = loopIndex * config.BATCH_SIZE + batchOffset;
          addOneRowIntoBatch(batch, stepOffset);
      }
      for (long batchOffset = barrier + 1; batchOffset < config.BATCH_SIZE; batchOffset++) {
          stepOffset = loopIndex * config.BATCH_SIZE + batchOffset;
          addOneRowIntoBatch(batch, stepOffset);
      }
      batch.setDeviceSchema(deviceSchema);
      return batch;
  }

  private Batch getDistOutOfOrderBatch(DeviceSchema deviceSchema) {
    Batch batch = new Batch();
    PoissonDistribution poissonDistribution = new PoissonDistribution(poissonRandom);
    int nextDelta;
    long stepOffset;
    for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
      if (probTool.returnTrueByProb(config.OVERFLOW_RATIO, poissonRandom)) {
        // generate overflow timestamp
        nextDelta = poissonDistribution.getNextPossionDelta();
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

  static void addOneRowIntoBatch(Batch batch, long stepOffset) {
    List<String> values = new ArrayList<>();
    long currentTimestamp = getCurrentTimestamp(stepOffset);
    for(int i = 0;i < config.SENSOR_NUMBER;i++) {
      values.add(workloadValues[i][(int)(Math.abs(stepOffset) % config.WORKLOAD_BUFFER_SIZE)]);
    }
    batch.add(currentTimestamp, values);
  }

  @Override
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
    for (int m = 0; m < config.DEVICE_NUMBER * config.REAL_INSERT_RATE; m++) {
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
    return Constants.START_TIMESTAMP * timeStampConst + timestampOffset;
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
    return new GroupByQuery(queryDevices, startTimestamp, endTimestamp,
        config.QUERY_AGGREGATE_FUN, config.TIME_UNIT);
  }

  public LatestPointQuery getLatestPointQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new LatestPointQuery(queryDevices, startTimestamp, endTimestamp,
        config.QUERY_AGGREGATE_FUN);
  }

  private static long getTimestampConst(String timePrecision){
    if(timePrecision.equals("ms")) {
      return 1L;
    } else if(timePrecision.equals("us")) {
      return 1000L;
    } else {
      return 1000000L;
    }
  }
}
