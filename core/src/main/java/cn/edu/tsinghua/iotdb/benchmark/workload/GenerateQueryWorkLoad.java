package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.enums.DBSwitch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class GenerateQueryWorkLoad extends QueryWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateQueryWorkLoad.class);

  private static final Random queryDeviceRandom = new Random(config.getQUERY_SEED());
  private static final long timeStampConst = getTimestampConst(config.getTIMESTAMP_PRECISION());
  private AtomicInteger nowDeviceId = new AtomicInteger(config.getFIRST_DEVICE_INDEX());
  private Long currentTimestamp = null;

  private final Map<Operation, Long> operationLoops;

  public GenerateQueryWorkLoad() {
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
    DeviceSchema deviceSchema = new DeviceSchema(deviceId, config.getSENSORS());
    return new DeviceQuery(deviceSchema);
  }

  @Override
  public void updateTime(long currentTimestamp) {
    if(currentTimestamp > 0){
      this.currentTimestamp = currentTimestamp;
    }
  }

  private long getQueryStartTimestamp(Operation operation) {
    if (currentTimestamp != null) {
      return currentTimestamp;
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
      List<Sensor> sensors = config.getSENSORS();
      Collections.shuffle(sensors, queryDeviceRandom);
      List<Sensor> querySensors = new ArrayList<>();
      for (int i = 0;
          querySensors.size() < config.getQUERY_SENSOR_NUM() && i < sensors.size();
          i++) {
        Sensor sensor = sensors.get(i);
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
