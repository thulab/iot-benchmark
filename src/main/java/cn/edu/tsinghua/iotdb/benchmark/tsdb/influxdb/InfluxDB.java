package cn.edu.tsinghua.iotdb.benchmark.tsdb.influxdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.model.InfluxDataModel;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
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
  private static final long TIMESTAMP_TO_NANO = (config.TIMESTAMP_PRECISION.equals("ms")) ?
          MILLIS_TO_NANO : 1;

  /**
   * constructor.
   */
  public InfluxDB() {
    influxUrl = config.DB_URL;
    influxDbName = config.DB_NAME;
    dataType = config.DATA_TYPE.toLowerCase();
  }

  @Override
  public void init() throws TsdbException {
    try {
      OkHttpClient.Builder client = new Builder().connectTimeout(5, TimeUnit.MINUTES).
          readTimeout(5, TimeUnit.MINUTES).writeTimeout(5, TimeUnit.MINUTES).
          retryOnConnectionFailure(true);
      influxDbInstance = org.influxdb.InfluxDBFactory.connect(influxUrl, client);
    } catch (Exception e) {
      LOGGER.error("Initialize InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
  }


  @Override
  public void cleanup() throws TsdbException {
    try {
      if (influxDbInstance.databaseExists(influxDbName)) {
        influxDbInstance.deleteDatabase(influxDbName);
      }

      // wait for deletion complete
      LOGGER.info("Waiting {}ms for old data deletion.", config.INIT_WAIT_TIME);
      Thread.sleep(config.INIT_WAIT_TIME);
    } catch (Exception e) {
      LOGGER.error("Cleanup InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() {
    if (influxDbInstance != null) {
      influxDbInstance.close();
    }
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    try {
      influxDbInstance.createDatabase(influxDbName);
    } catch (Exception e) {
      LOGGER.error("RegisterSchema InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    BatchPoints batchPoints = BatchPoints.database(influxDbName)
        .retentionPolicy(defaultRp)
        .consistency(org.influxdb.InfluxDB.ConsistencyLevel.ALL).build();
    try {
      InfluxDataModel model;
      for (Record record : batch.getRecords()) {
        model = createDataModel(batch.getDeviceSchema(), record.getTimestamp(),
            record.getRecordDataValue());
        batchPoints.point(model.toInfluxPoint());
      }

      influxDbInstance.write(batchPoints);

      return new Status(true);
    } catch (Exception e) {
      LOGGER.warn(e.getMessage());
      return new Status(false, 0, e, e.toString());
    }
  }

  /**
   * eg. SELECT s_0 FROM group_2  WHERE ( device = 'd_8' ) AND time = 1535558405000000000.
   */
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    String sql = getPreciseQuerySql(preciseQuery);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * eg. SELECT s_0 FROM group_2  WHERE ( device = 'd_8' ) AND time >= 1535558405000000000 AND time
   * <= 153555800000.
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    String rangeQueryHead = getSimpleQuerySqlHead(rangeQuery.getDeviceSchema());
    String sql = addWhereTimeClause(rangeQueryHead, rangeQuery);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * eg. SELECT s_3 FROM group_0  WHERE ( device = 'd_3' ) AND time >= 1535558420000000000 AND time
   * <= 153555800000 AND s_3 > -5.0.
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    String rangeQueryHead = getSimpleQuerySqlHead(valueRangeQuery.getDeviceSchema());
    String sqlWithTimeFilter = addWhereTimeClause(rangeQueryHead, valueRangeQuery);
    String sqlWithValueFilter = addWhereValueClause(valueRangeQuery.getDeviceSchema(),
        sqlWithTimeFilter, valueRangeQuery.getValueThreshold());
    return executeQueryAndGetStatus(sqlWithValueFilter);
  }

  /**
   * eg. SELECT count(s_3) FROM group_4  WHERE ( device = 'd_16' ) AND time >= 1535558410000000000
   * AND time <=8660000000000.
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(),
        aggRangeQuery.getAggFun());
    String sql = addWhereTimeClause(aggQuerySqlHead, aggRangeQuery);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * eg. SELECT count(s_3) FROM group_3  WHERE ( device = 'd_12' ) AND s_3 > -5.0.
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(aggValueQuery.getDeviceSchema(),
        aggValueQuery.getAggFun());
    String sql = addWhereValueClause(aggValueQuery.getDeviceSchema(), aggQuerySqlHead,
        aggValueQuery.getValueThreshold());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * eg. SELECT count(s_1) FROM group_2  WHERE ( device = 'd_8' ) AND time >= 1535558400000000000
   * AND time <= 650000000000 AND s_1 > -5.0.
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    String rangeQueryHead = getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(),
        aggRangeValueQuery.getAggFun());
    String sqlWithTimeFilter = addWhereTimeClause(rangeQueryHead, aggRangeValueQuery);
    String sqlWithValueFilter = addWhereValueClause(aggRangeValueQuery.getDeviceSchema(),
        sqlWithTimeFilter, aggRangeValueQuery.getValueThreshold());
    return executeQueryAndGetStatus(sqlWithValueFilter);
  }

  /**
   * eg. SELECT count(s_3) FROM group_4  WHERE ( device = 'd_16' ) AND time >= 1535558430000000000
   * AND time <=8680000000000 GROUP BY time(20000ms).
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    String sqlHeader = getAggQuerySqlHead(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
    String sqlWithTimeFilter = addWhereTimeClause(sqlHeader, groupByQuery);
    String sqlWithGroupBy = addGroupByClause(sqlWithTimeFilter, groupByQuery.getGranularity());
    return executeQueryAndGetStatus(sqlWithGroupBy);
  }

  /**
   * eg. SELECT last(s_2) FROM group_2  WHERE ( device = 'd_8' ).
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    String sql = getAggQuerySqlHead(latestPointQuery.getDeviceSchema(), "last");
    return executeQueryAndGetStatus(sql);
  }

  private InfluxDataModel createDataModel(DeviceSchema deviceSchema, Long time,
      List<String> valueList)
      throws TsdbException {
    InfluxDataModel model = new InfluxDataModel();
    model.measurement = deviceSchema.getGroup();
    model.tagSet.put("device", deviceSchema.getDevice());
    model.timestamp = time;
    model.timestampPrecision = config.TIMESTAMP_PRECISION;
    List<String> sensors = deviceSchema.getSensors();
    for (int i = 0; i < sensors.size(); i++) {
      Number value = parseNumber(valueList.get(i));
      model.fields.put(sensors.get(i), value);
    }
    return model;
  }

  private Number parseNumber(String value) throws TsdbException {
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
        throw new TsdbException("unsuport datatype " + dataType);

    }
  }

  private Status executeQueryAndGetStatus(String sql) {
    LOGGER.debug("{} query SQL: {}", Thread.currentThread().getName(), sql);

    QueryResult results = influxDbInstance.query(new Query(sql, influxDbName));
    int cnt = 0;
    for (Result result : results.getResults()) {
      List<Series> series = result.getSeries();
      if (series == null) {
        continue;
      }
      if (result.getError() != null) {
        return new Status(false, cnt, new Exception(result.getError()), sql);
      }
      for (Series serie : series) {
        List<List<Object>> values = serie.getValues();
        cnt += values.size() * (serie.getColumns().size() - 1);
      }
    }

    LOGGER.debug("{} 查到数据点数: {}", Thread.currentThread().getName(), cnt);
    return new Status(true, cnt);
  }

  private static String getPreciseQuerySql(PreciseQuery preciseQuery) {
    String strTime = "" + preciseQuery.getTimestamp() * TIMESTAMP_TO_NANO;
    return getSimpleQuerySqlHead(preciseQuery.getDeviceSchema()) + " AND time = " + strTime;
  }

  /**
   * add time filter for query statements.
   *
   * @param sql sql header
   * @param rangeQuery range query
   * @return sql with time filter
   */
  private static String addWhereTimeClause(String sql, RangeQuery rangeQuery) {
    String startTime = "" + rangeQuery.getStartTimestamp() * TIMESTAMP_TO_NANO;
    String endTime = "" + rangeQuery.getEndTimestamp() * TIMESTAMP_TO_NANO;
    return sql + " AND time >= " + startTime
        + " AND time <= " + endTime;
  }

  /**
   * add value filter for query statements.
   *
   * @param devices query device schema
   * @param sqlHeader sql header
   * @param valueThreshold lower bound of query value filter
   * @return sql with value filter
   */
  private static String addWhereValueClause(List<DeviceSchema> devices, String sqlHeader,
      double valueThreshold) {
    StringBuilder builder = new StringBuilder(sqlHeader);
    for (String sensor : devices.get(0).getSensors()) {
      builder.append(" AND ").append(sensor).append(" > ").append(valueThreshold);
    }
    return builder.toString();
  }

  /**
   * add group by clause for query.
   *
   * @param sqlHeader sql header
   * @param timeGranularity time granularity of group by
   */
  private static String addGroupByClause(String sqlHeader, long timeGranularity) {
    return sqlHeader + " GROUP BY time(" + timeGranularity + config.TIMESTAMP_PRECISION +")";
  }

  /**
   * generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT s_0, s_3 FROM root.group_0, root.group_1
   * WHERE(device='d_0' OR device='d_1')
   */
  private static String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();

    builder.append(querySensors.get(0));
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i));
    }

    builder.append(generateConstrainForDevices(devices));
    return builder.toString();
  }

  /**
   * generate aggregation query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT count(s_0), count(s_3) FROM root.group_0, root.group_1
   * WHERE(device='d_0' OR device='d_1')
   */
  private static String getAggQuerySqlHead(List<DeviceSchema> devices, String method) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();

    builder.append(method).append("(").append(querySensors.get(0)).append(")");
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(method).append("(").append(querySensors.get(i)).append(")");
    }

    builder.append(generateConstrainForDevices(devices));
    return builder.toString();
  }

  /**
   * generate from and where clause for specified devices.
   *
   * @param devices schema list of query devices
   * @return from and where clause
   */
  private static String generateConstrainForDevices(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    Set<String> groups = new HashSet<>();
    for (DeviceSchema d : devices) {
      groups.add(d.getGroup());
    }
    builder.append(" FROM ");
    for (String g : groups) {
      builder.append(g).append(" , ");
    }
    builder.deleteCharAt(builder.lastIndexOf(","));
    builder.append("WHERE (");
    for (DeviceSchema d : devices) {
      builder.append(" device = '").append(d.getDevice()).append("' OR");
    }
    builder.delete(builder.lastIndexOf("OR"), builder.length());
    builder.append(")");

    return builder.toString();
  }

}