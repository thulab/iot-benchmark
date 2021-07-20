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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO 阅读
public class SyntheticWorkload implements IWorkload {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyntheticWorkload.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Random timestampRandom = new Random(config.getDATA_SEED());
  private final ProbTool probTool;
  private final Map<DeviceSchema, Long> maxTimestampIndexMap;
  private final Random poissonRandom;
  private final Random queryDeviceRandom;
  private final Map<Operation, Long> operationLoops;
  private static final Random random = new Random(config.getDATA_SEED());
  private static final String DECIMAL_FORMAT = "%." + config.getNUMBER_OF_DECIMAL_DIGIT() + "f";
  private static final Random dataRandom = new Random(config.getDATA_SEED());
  // this must before the initWorkloadValues function calls TODO rename to valueScaleFactor 删除 * 10
  private static int scaleFactor = 10;
  /**workloadValues[传感器][序号]。 对于那些有规律的数据，存储了每个传感器的一段数据，用于按规律快速生成*/
  private static final Object[][] workloadValues = initWorkloadValues();
  private static final String CHAR_TABLE =
      "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final long timeStampConst = getTimestampConst(config.getTIMESTAMP_PRECISION());

  public SyntheticWorkload(int clientId) {
    probTool = new ProbTool();
    maxTimestampIndexMap = new HashMap<>();
    poissonRandom = new Random(config.getDATA_SEED());
    for(DeviceSchema schema: DataSchema.getInstance().getClientBindSchema().get(clientId)) {
      maxTimestampIndexMap.put(schema, 0L);
    }
    queryDeviceRandom = new Random(config.getQUERY_SEED() + clientId);
    operationLoops = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLoops.put(operation, 0L);
    }
  }

  // TODO 移除精度
  private static void initScaleFactor() {
    for (int i = 0; i < ConfigDescriptor.getInstance().getConfig().getNUMBER_OF_DECIMAL_DIGIT(); i++) {
      scaleFactor *= 10;
    }
  }

  private static Object[][] initWorkloadValues() {
    initScaleFactor();
    Object[][] workloadValues = null;
    if (!config.getOPERATION_PROPORTION().split(":")[0].equals("0")) {
      //不为0就是有写入操作。
      workloadValues = new Object[config.getSENSOR_NUMBER()][config.getWORKLOAD_BUFFER_SIZE()];
      int sensorIndex = 0;
      for (int j = 0; j < config.getSENSOR_NUMBER(); j++) {
        String sensor = config.SENSOR_CODES.get(j);
        for (int i = 0; i < config.getWORKLOAD_BUFFER_SIZE(); i++) {
          //这个时间戳只用来生成有周期性的数据。所以时间戳也是周期的。
          long currentTimestamp = getCurrentTimestamp(i);
          Object value;
          if (getNextDataType(sensorIndex).equals("TEXT")) {
            //TEXT case: pick NUMBER_OF_DECIMAL_DIGIT chars to be a String for insertion.
            StringBuilder builder = new StringBuilder(config.getNUMBER_OF_DECIMAL_DIGIT());
            for (int k = 0; k < config.getNUMBER_OF_DECIMAL_DIGIT(); k++) {
              assert dataRandom != null;
              builder.append(CHAR_TABLE.charAt(dataRandom.nextInt(CHAR_TABLE.length())));
            }
            value = builder.toString();
          } else {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            Number number = Function.getValueByFuntionidAndParam(param, currentTimestamp);
            switch (getNextDataType(sensorIndex)) {
              case "BOOLEAN":
                // TODO 500 -> avg
                value = number.floatValue() > 500;
                break;
              case "INT32":
                value = number.intValue();
                break;
              case "INT64":
                value = number.longValue();
                break;
              case "FLOAT":
                // TODO
                value = ((float) (Math.round(number.floatValue() * scaleFactor))) / scaleFactor;
                break;
              case "DOUBLE":
                // TODO
                value = ((double) Math.round(number.doubleValue() * scaleFactor)) / scaleFactor;
                break;
              default:
                value = null;
                break;
            }
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
    double[] p = new double[6 + 1];
    p[0] = 0.0;
    // split [0,1] to n regions, each region corresponds to a data type whose proportion
    // is the region range size.
    for (int i = 1; i <= 6; i++) {
      p[i] = p[i - 1] + proportion.get(i - 1);
    }
    double sensorPosition = sensorIndex * 1.0 / config.getSENSOR_NUMBER();
    int i;
    for (i = 1; i <= 6; i++) {
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
    String[] split = config.getINSERT_DATATYPE_PROPORTION().split(":");
    if (split.length != 6) {
      LOGGER.error("INSERT_DATATYPE_PROPORTION error, please check this parameter.");
    }
    double[] proportions = new double[6];
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
    long timeStampOffset = config.getPOINT_STEP() * stepOffset;
    if (config.isIS_OVERFLOW()) {
      // 随机加上数秒，使得时间不是均匀的。但是不会乱序。
      // 添加 ratio 使用randorm->timestampRandom
      //TODO 但是方法名可是说可能会乱序啊！！！！！！！
      timeStampOffset += (long) (random.nextDouble() * config.getPOINT_STEP());
    } else {
      if (config.isIS_RANDOM_TIMESTAMP_INTERVAL()) {
        //TODO 这方法跟上面有啥区别？？？
        timeStampOffset += (long) (config.getPOINT_STEP() * timestampRandom.nextDouble());
      }
    }
    //TODO 为啥timeStampOffset不乘以时间精度？？？
    return Constants.START_TIMESTAMP * timeStampConst + timeStampOffset;
  }

  private Batch getOrderedBatch(DeviceSchema deviceSchema, long loopIndex) {
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE(); batchOffset++) {
      long stepOffset = loopIndex * config.getBATCH_SIZE() + batchOffset;
      addOneRowIntoBatch(batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  // TODO 为啥插入一个点
  private Batch getOrderedBatch(DeviceSchema deviceSchema, long loopIndex,int colIndex) {
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE(); batchOffset++) {
      long stepOffset = loopIndex * config.getBATCH_SIZE() + batchOffset;
      addOneRowIntoBatch(batch, stepOffset,colIndex);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  private Batch getLocalOutOfOrderBatch(DeviceSchema deviceSchema, long loopIndex) {
    // TODO 修改乱序的比例
    // Config 添加乱序步长 Poisson 替代MAX_K
    // 乱序步长为k，则在t-k的范围内按照分布规律进行差值，大于t的部分不变，t为当前时间
    Batch batch = new Batch();
    long barrier = (long) (config.getBATCH_SIZE() * config.getOVERFLOW_RATIO());
    long stepOffset = loopIndex * config.getBATCH_SIZE() + barrier;
    // move data(index = barrier) to front
    addOneRowIntoBatch(batch, stepOffset);
    for (long batchOffset = 0; batchOffset < barrier; batchOffset++) {
      stepOffset = loopIndex * config.getBATCH_SIZE() + batchOffset;
      addOneRowIntoBatch(batch, stepOffset);
    }
    for (long batchOffset = barrier + 1; batchOffset < config.getBATCH_SIZE(); batchOffset++) {
      stepOffset = loopIndex * config.getBATCH_SIZE() + batchOffset;
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
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE(); batchOffset++) {
      if (probTool.returnTrueByProb(config.getOVERFLOW_RATIO(), poissonRandom)) {
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
    List<Object> values = new ArrayList<>();
    long currentTimestamp = getCurrentTimestamp(stepOffset);
    for(int i = 0;i < config.getSENSOR_NUMBER();i++) {
        values.add(workloadValues[i][(int)(Math.abs(stepOffset) % config.getWORKLOAD_BUFFER_SIZE())]);
    }
    batch.add(currentTimestamp, values);
  }

  /**
   * 该方法仅填充一个值进入values TODO check
   * @param batch
   * @param stepOffset
   * @param colIndex
   */
  static void addOneRowIntoBatch(Batch batch, long stepOffset,int colIndex) {
    List<Object> values = new ArrayList<>();
    long currentTimestamp = getCurrentTimestamp(stepOffset);
    values.add(workloadValues[colIndex][(int)(Math.abs(stepOffset) % config.getWORKLOAD_BUFFER_SIZE())]);
    batch.add(currentTimestamp, values);
  }

  @Override
  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException {
    if (!config.isIS_OVERFLOW()) {
      return getOrderedBatch(deviceSchema, loopIndex);
    } else {
      switch (config.getOVERFLOW_MODE()) {
        case 0:
          return getDistOutOfOrderBatch(deviceSchema);
        case 1:
          return getLocalOutOfOrderBatch(deviceSchema, loopIndex);
        default:
          throw new WorkloadException("Unsupported overflow mode: " + config.getOVERFLOW_MODE());
      }
    }
  }

  @Override
  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex,int colIndex) throws WorkloadException {
    if (!config.isIS_OVERFLOW()) {
      return getOrderedBatch(deviceSchema, loopIndex,colIndex);
    } else {
      switch (config.getOVERFLOW_MODE()) {
        case 0:
          return getDistOutOfOrderBatch(deviceSchema);
        case 1:
          return getLocalOutOfOrderBatch(deviceSchema, loopIndex);
        default:
          throw new WorkloadException("Unsupported overflow mode: " + config.getOVERFLOW_MODE());
      }
    }
  }

  private List<DeviceSchema> getQueryDeviceSchemaList() throws WorkloadException {
    checkQuerySchemaParams();
    List<DeviceSchema> queryDevices = new ArrayList<>();
    List<Integer> clientDevicesIndex = new ArrayList<>();
    for (int m = 0; m < config.getDEVICE_NUMBER() * config.getREAL_INSERT_RATE(); m++) {
      clientDevicesIndex.add(m);
    }
    Collections.shuffle(clientDevicesIndex, queryDeviceRandom);
    for (int m = 0; m < config.getQUERY_DEVICE_NUM(); m++) {
      DeviceSchema deviceSchema = new DeviceSchema(clientDevicesIndex.get(m));
      List<String> sensors = deviceSchema.getSensors();
      Collections.shuffle(sensors, queryDeviceRandom);
      List<String> querySensors = new ArrayList<>();
      for (int i = 0; i < config.getQUERY_SENSOR_NUM(); i++) {
        querySensors.add(sensors.get(i));
      }
      deviceSchema.setSensors(querySensors);
      queryDevices.add(deviceSchema);
    }
    return queryDevices;
  }

  private void checkQuerySchemaParams() throws WorkloadException {
    if (!(config.getQUERY_DEVICE_NUM() > 0 && config.getQUERY_DEVICE_NUM() <= config.getDEVICE_NUMBER())) {
      throw new WorkloadException("getQUERY_DEVICE_NUM() is not correct, please check.");
    }
    if (!(config.getQUERY_SENSOR_NUM() > 0 && config.getQUERY_SENSOR_NUM() <= config.getSENSOR_NUMBER())) {
      throw new WorkloadException("QUERY_SENSOR_NUM is not correct, please check.");
    }
  }

  private long getQueryStartTimestamp() {
    long currentQueryLoop = operationLoops.get(Operation.PRECISE_QUERY);
    long timestampOffset = currentQueryLoop * config.getSTEP_SIZE() * config.getPOINT_STEP();
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
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new RangeQuery(queryDevices, startTimestamp, endTimestamp);
  }

  public ValueRangeQuery getValueRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new ValueRangeQuery(queryDevices, startTimestamp, endTimestamp,
        config.getQUERY_LOWER_LIMIT());
  }

  public AggRangeQuery getAggRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new AggRangeQuery(queryDevices, startTimestamp, endTimestamp,
        config.getQUERY_AGGREGATE_FUN());
  }

  public AggValueQuery getAggValueQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    return new AggValueQuery(queryDevices, config.getQUERY_AGGREGATE_FUN(), config.getQUERY_LOWER_LIMIT());
  }

  public AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new AggRangeValueQuery(queryDevices, startTimestamp, endTimestamp,
        config.getQUERY_AGGREGATE_FUN(), config.getQUERY_LOWER_LIMIT());
  }

  @Override
  public GroupByQuery getGroupByQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new GroupByQuery(queryDevices, startTimestamp, endTimestamp,
        config.getQUERY_AGGREGATE_FUN(), config.getTIME_UNIT());
  }

  public LatestPointQuery getLatestPointQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.getQUERY_INTERVAL();
    return new LatestPointQuery(queryDevices, startTimestamp, endTimestamp,
        config.getQUERY_AGGREGATE_FUN());
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

