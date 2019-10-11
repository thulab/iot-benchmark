package cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private SimpleDateFormat sdf;
  private static final String CREATE_SERIES_SQL =
      "CREATE TIMESERIES %s WITH DATATYPE=%s,ENCODING=%s,COMPRESSOR=%s";
  private static final String SET_STORAGE_GROUP_SQL = "SET STORAGE GROUP TO %s";
  private Connection connection;

  public IoTDB() {
    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  @Override
  public void init() throws TsdbException {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      connection = DriverManager
          .getConnection(String.format(Constants.URL, config.HOST, config.PORT), Constants.USER,
              Constants.PASSWD);
    } catch (Exception e) {
      LOGGER.error("Initialize IoTDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    // currently no implementation
  }

  @Override
  public void close() throws TsdbException {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        LOGGER.error("Failed to close IoTDB connection because ", e);
        throw new TsdbException(e);
      }
    }
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    int count = 0;

    try {
      // get all storage groups
      Set<String> groups = new HashSet<>();
      for (DeviceSchema schema : schemaList) {
        groups.add(schema.getGroup());
      }
      // register storage groups
      try (Statement statement = connection.createStatement()) {
        for (String group : groups) {
          statement.addBatch(
              String.format(SET_STORAGE_GROUP_SQL, Constants.ROOT_SERIES_NAME + "." + group));
        }
        statement.executeBatch();
        statement.clearBatch();
      }
    } catch (SQLException e) {
      LOGGER.error("Set storage group failed because ", e);
      throw new TsdbException(e);
    }
    // create time series
    try (Statement statement = connection.createStatement()) {
      for (DeviceSchema deviceSchema : schemaList) {
        for (String sensor : deviceSchema.getSensors()) {
          String createSeriesSql = String.format(CREATE_SERIES_SQL,
              Constants.ROOT_SERIES_NAME
                  + "." + deviceSchema.getGroup()
                  + "." + deviceSchema.getDevice()
                  + "." + sensor,
              config.DATA_TYPE, config.ENCODING, config.COMPRESSOR);
          statement.addBatch(createSeriesSql);
          count++;
          if (count % 5000 == 0) {
            statement.executeBatch();
            statement.clearBatch();
          }
        }

      }
      statement.executeBatch();
      statement.clearBatch();
    } catch (SQLException e) {
      LOGGER.error("Register IoTDB schema failed because ", e);
      throw new TsdbException(e);
    }

  }


  @Override
  public Status insertOneBatch(Batch batch) {
    long st;
    long en;
    try (Statement statement = connection.createStatement()) {
      for (Record record : batch.getRecords()) {
        String sql = getInsertOneBatchSql(batch.getDeviceSchema(), record.getTimestamp(),
            record.getRecordDataValue());
        statement.addBatch(sql);
      }
      st = System.nanoTime();
      statement.executeBatch();
      en = System.nanoTime();
      return new Status(true, en - st);
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
    String sql = getRangeQuerySql(rangeQuery.getDeviceSchema(), rangeQuery.getStartTimestamp(),
        rangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT s_39 FROM root.group_2.d_29
   * WHERE time >= 2010-01-01 12:00:00
   * AND time <= 2010-01-01 12:30:00
   * AND root.group_2.d_29.s_39 > 0.0
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    String sql = getvalueRangeQuerySql(valueRangeQuery);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT max_value(s_76) FROM root.group_3.d_31
   * WHERE time >= 2010-01-01 12:00:00
   * AND time <= 2010-01-01 12:30:00
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(),
        aggRangeQuery.getAggFun());
    String sql = addWhereTimeClause(aggQuerySqlHead, aggRangeQuery.getStartTimestamp(),
        aggRangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT max_value(s_39) FROM root.group_2.d_29
   * WHERE root.group_2.d_29.s_39 > 0.0
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(aggValueQuery.getDeviceSchema(),
        aggValueQuery.getAggFun());
    String sql = aggQuerySqlHead + " WHERE " + getValueFilterClause(aggValueQuery.getDeviceSchema(),
        aggValueQuery.getValueThreshold()).substring(4);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT max_value(s_39) FROM root.group_2.d_29 WHERE time >= 2010-01-01 12:00:00 AND
   * time <= 2010-01-01 12:30:00 AND root.group_2.d_29.s_39 > 0.0
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(),
        aggRangeValueQuery.getAggFun());
    String sql = addWhereTimeClause(aggQuerySqlHead, aggRangeValueQuery.getStartTimestamp(),
        aggRangeValueQuery.getEndTimestamp());
    sql += getValueFilterClause(aggRangeValueQuery.getDeviceSchema(),
        aggRangeValueQuery.getValueThreshold());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * select aggFun(sensor) from device group by(interval, startTimestamp, [startTimestamp,
   * endTimestamp])
   * example: SELECT max_value(s_81) FROM root.group_9.d_92
   * GROUP BY(600000ms, 1262275200000,[2010-01-01 12:00:00,2010-01-01 13:00:00])
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(groupByQuery.getDeviceSchema(),
        groupByQuery.getAggFun());
    String sql = addGroupByClause(aggQuerySqlHead, groupByQuery.getStartTimestamp(),
        groupByQuery.getEndTimestamp(), groupByQuery.getGranularity());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT max_time(s_76) FROM root.group_3.d_31
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(latestPointQuery.getDeviceSchema(), "max_time");
    return executeQueryAndGetStatus(aggQuerySqlHead);
  }

  private String getvalueRangeQuerySql(ValueRangeQuery valueRangeQuery) {
    String rangeQuerySql = getRangeQuerySql(valueRangeQuery.getDeviceSchema(),
        valueRangeQuery.getStartTimestamp(), valueRangeQuery.getEndTimestamp());
    String valueFilterClause = getValueFilterClause(valueRangeQuery.getDeviceSchema(),
        valueRangeQuery.getValueThreshold());
    return rangeQuerySql + valueFilterClause;
  }

  private String getValueFilterClause(List<DeviceSchema> deviceSchemas, double valueThreshold) {
    StringBuilder builder = new StringBuilder();
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        builder.append(" AND ").append(getDevicePath(deviceSchema)).append(".")
            .append(sensor).append(" > ")
            .append(valueThreshold);
      }
    }
    return builder.toString();
  }

  private String getInsertOneBatchSql(DeviceSchema deviceSchema, long timestamp,
      List<String> values) {
    StringBuilder builder = new StringBuilder();
    builder.append("insert into ")
        .append(Constants.ROOT_SERIES_NAME)
        .append(".").append(deviceSchema.getGroup())
        .append(".").append(deviceSchema.getDevice())
        .append("(timestamp");
    for (String sensor : deviceSchema.getSensors()) {
      builder.append(",").append(sensor);
    }
    builder.append(") values(");
    builder.append(timestamp);
    for (String value : values) {
      builder.append(",").append(value);
    }
    builder.append(")");
    LOGGER.debug("getInsertOneBatchSql: {}", builder);
    return builder.toString();
  }

  /**
   * generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT s_0, s_3 FROM root.group_0.d_1, root.group_1.d_2
   */
  private String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();
    builder.append(querySensors.get(0));
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i));
    }
    return addFromClause(devices, builder);
  }

  private String getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();
    builder.append(aggFun).append("(").append(querySensors.get(0)).append(")");
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(aggFun).append("(").append(querySensors.get(i)).append(")");
    }
    return addFromClause(devices, builder);
  }

  private String addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    builder.append(" FROM ").append(getDevicePath(devices.get(0)));
    for (int i = 1; i < devices.size(); i++) {
      builder.append(", ").append(getDevicePath(devices.get(i)));
    }
    return builder.toString();
  }

  // convert deviceSchema to the format: root.group_1.d_1
  private String getDevicePath(DeviceSchema deviceSchema) {
    return Constants.ROOT_SERIES_NAME + "." + deviceSchema.getGroup() + "." + deviceSchema
        .getDevice();
  }

  private String getPreciseQuerySql(PreciseQuery preciseQuery) {
    String strTime = sdf.format(new Date(preciseQuery.getTimestamp()));
    return getSimpleQuerySqlHead(preciseQuery.getDeviceSchema()) + " WHERE time = " + strTime;
  }

  private Status executeQueryAndGetStatus(String sql) {
    LOGGER.info("{} 提交执行的查询SQL: {}", Thread.currentThread().getName(), sql);
    long st;
    long en;
    int line = 0;
    int queryResultPointNum = 0;
    try (Statement statement = connection.createStatement()) {
      st = System.nanoTime();
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          line++;
        }
      }
      en = System.nanoTime();
      queryResultPointNum = line * config.QUERY_SENSOR_NUM;
      return new Status(true, en - st, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, 0, queryResultPointNum, e, sql);
    } catch (Throwable t) {
      return new Status(false, 0, queryResultPointNum, new Exception(t), sql);
    }
  }

  private String getRangeQuerySql(List<DeviceSchema> deviceSchemas, long start, long end) {
    return addWhereTimeClause(getSimpleQuerySqlHead(deviceSchemas), start, end);
  }

  private String addWhereTimeClause(String prefix, long start, long end) {
    String startTime = sdf.format(new Date(start));
    String endTime = sdf.format(new Date(end));
    return prefix + " WHERE time >= " + startTime + " AND time <= " + endTime;
  }

  private String addGroupByClause(String prefix, long start, long end, long granularity) {
    StringBuilder builder = new StringBuilder(prefix);
    String startTime = sdf.format(new Date(start));
    String endTime = sdf.format(new Date(end));
    builder.append(" GROUP BY(").append(granularity).append("ms, ").append(start);
    builder.append(",[").append(startTime).append(",").append(endTime).append("]").append(")");
    return builder.toString();
  }
}
