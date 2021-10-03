package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IQueryWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryWorkLoad implements IQueryWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryWorkLoad.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
  private static final Random queryDeviceRandom = new Random(config.getQUERY_SEED());
  private static final long timeStampConst = getTimestampConst(config.getTIMESTAMP_PRECISION());
  private static AtomicInteger nowDeviceId = new AtomicInteger(config.getFIRST_DEVICE_INDEX());

  private final Map<Operation, Long> operationLoops;

  public static IQueryWorkLoad getInstance() {
    return new QueryWorkLoad();
  }

  public QueryWorkLoad() {
    operationLoops = new EnumMap<>(Operation.class);
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
    DeviceSchema deviceSchema = new DeviceSchema(deviceId, config.getSENSOR_CODES());
    return new DeviceQuery(deviceSchema);
  }

  private long getQueryStartTimestamp(Operation operation) {
    long currentQueryLoop = operationLoops.get(operation);
    long timestampOffset = currentQueryLoop * config.getSTEP_SIZE() * config.getPOINT_STEP();
    operationLoops.put(operation, currentQueryLoop + 1);
    return Constants.START_TIMESTAMP * timeStampConst + timestampOffset;
  }

  /**
   * Return the list of deviceSchema
   *
   * @param typeAllow true: allow generating bool and text type.
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
          SensorType sensorType = metaDataSchema.getSensorType(device, sensor);
          if (sensorType == SensorType.BOOLEAN || sensorType == SensorType.TEXT) {
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
