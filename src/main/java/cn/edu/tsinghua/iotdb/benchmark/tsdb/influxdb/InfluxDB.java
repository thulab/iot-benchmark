package cn.edu.tsinghua.iotdb.benchmark.tsdb.influxdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.model.InfluxDataModel;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
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
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDB.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  private final String influxUrl;
  private final String influxDbName;
  private final String defaultRp = "autogen";
  private final String dataType;

  private org.influxdb.InfluxDB influxDbInstance;
  private static final int MILLIS_TO_NANO = 1000000;

  /**
   * constructor.
   */
  public InfluxDB() {
    influxUrl = config.DB_URL;
    influxDbName = config.DB_NAME;
    dataType = config.DATA_TYPE.toLowerCase();
    influxDbInstance = org.influxdb.InfluxDBFactory.connect(influxUrl);
  }

  @Override
  public void init() throws SQLException {
    //delete old data
    if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_QUERY_TEST_WITH_DEFAULT_PATH)) {
      if (!influxDbInstance.databaseExists(influxDbName)) {
        throw new SQLException("要查询的数据库" + influxDbName + "不存在！");
      }
    } else {
      if (influxDbInstance.databaseExists(influxDbName)) {
        influxDbInstance.deleteDatabase(influxDbName);
      }
      createDatabase(influxDbName);
    }
    // wait for deletion complete
    try {
      LOGGER.info("Waiting {}ms for old data deletion.", config.INIT_WAIT_TIME);
      Thread.sleep(config.INIT_WAIT_TIME);
    } catch (InterruptedException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void cleanup() {
    if (influxDbInstance.databaseExists(influxDbName)) {
      influxDbInstance.deleteDatabase(influxDbName);
    }
    // wait for deletion complete
    try {
      LOGGER.info("Waiting {}ms for old data deletion.", config.INIT_WAIT_TIME);
      Thread.sleep(config.INIT_WAIT_TIME);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    influxDbInstance.close();
  }

  @Override
  public void registerSchema(Measurement measurement) {
    // no need for InfluxDB
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    BatchPoints batchPoints = BatchPoints.database(influxDbName).tag("async", "true")
        .retentionPolicy(defaultRp)
        .consistency(org.influxdb.InfluxDB.ConsistencyLevel.ALL).build();

    try {
      InfluxDataModel model;
      for (Entry<Long, List<String>> entry : batch.getRecords().entrySet()) {
        model = createDataModel(batch.getDeviceSchema(), entry.getKey(), entry.getValue());
        batchPoints.point(model.toInfluxPoint());
      }
      long startTime = System.nanoTime();
      influxDbInstance.write(batchPoints);
      long endTime = System.nanoTime();
      long latency = endTime - startTime;
      return new Status(true, latency);
    } catch (Exception e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    String sql = getPreciseQuerySql(preciseQuery);
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    String sql = getRangeQuerySql(rangeQuery);
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return null;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return null;
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return null;
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return null;
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return null;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return null;
  }

  /**
   * create database.
   *
   * @param databaseName database name
   */
  private void createDatabase(String databaseName) {
    influxDbInstance.createDatabase(databaseName);
  }

  private InfluxDataModel createDataModel(DeviceSchema deviceSchema, Long time,
      List<String> valueList)
      throws Exception {
    InfluxDataModel model = new InfluxDataModel();
    int deviceNum = deviceSchema.getDeviceId();
    int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
    int groupNum = deviceNum / groupSize;
    model.measurement = "group_" + groupNum;
    model.tagSet.put("device", deviceSchema.getDevice());
    model.timestamp = time;
    List<String> sensors = deviceSchema.getSensors();
    for (int i = 0; i < sensors.size(); i++) {
      Number value = parseNumber(valueList.get(i));
      model.fields.put(sensors.get(i), value);
    }
    return model;
  }

  private Number parseNumber(String value) throws Exception {
    switch (dataType) {
      case "float":
        return Float.parseFloat(value);
      case "double":
        return Double.parseDouble(value);
      case "int":
      case "int32":
      case "integer":
        return Integer.parseInt(value);
      case "int64":
      case "long":
        return Long.parseLong(value);
      default:
        throw new Exception("unsuport datatype " + dataType);

    }
  }

  private Status executeQueryAndGetStatus(String sql) {
    LOGGER.debug("{} 提交执行的查询SQL: {}", Thread.currentThread().getName(), sql);
    long startTimeStamp = System.nanoTime();
    QueryResult results = influxDbInstance.query(new Query(sql, influxDbName));
    int cnt = 0;
    for (Result result : results.getResults()) {
      List<Series> series = result.getSeries();
      if (series == null) {
        continue;
      }
      if (result.getError() != null) {
        return new Status(false, 0, cnt, new Exception(result.getError()), sql);
      }
      for (Series serie : series) {
        List<List<Object>> values = serie.getValues();
        cnt += values.size() * (serie.getColumns().size() - 1);
      }
    }
    long endTimeStamp = System.nanoTime();
    return new Status(true, endTimeStamp - startTimeStamp, cnt);
  }

  private static String getPreciseQuerySql(PreciseQuery preciseQuery) {
    String strTime = "" + preciseQuery.getTimestamp() * MILLIS_TO_NANO;
    return getSimpleQuerySqlHead(preciseQuery.getDeviceSchema()) + " AND time = " + strTime;
  }

  private static String getRangeQuerySql(RangeQuery rangeQuery) {
    String startTime = "" + rangeQuery.getStartTimestamp() * MILLIS_TO_NANO;
    String endTime = "" + rangeQuery.getEndTimestamp() * MILLIS_TO_NANO;
    return getSimpleQuerySqlHead(rangeQuery.getDeviceSchema()) + " AND time >= " + startTime
        + " AND time <= " + endTime;
  }

  /**
   * generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT s_0, s_3 FROM root.group_0, root.group_1
   *      WHERE(device='d_0' OR device='d_1')
   */
  private static String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();

    builder.append(querySensors.get(0));
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i));
    }

    Set<Integer> groups = new HashSet<>();
    for (DeviceSchema d : devices) {
      groups.add(d.getDeviceId() / (config.DEVICE_NUMBER / config.GROUP_NUMBER));
    }
    builder.append(" FROM ");
    for (int g : groups) {
      builder.append(" group_" + g).append(" , ");
    }
    builder.deleteCharAt(builder.lastIndexOf(","));
    builder.append(" WHERE (");
    for (DeviceSchema d : devices) {
      builder.append(" device = 'd_" + d.getDeviceId() + "' OR");
    }
    builder.delete(builder.lastIndexOf("OR"), builder.length());
    builder.append(")");

    return builder.toString();
  }

}
