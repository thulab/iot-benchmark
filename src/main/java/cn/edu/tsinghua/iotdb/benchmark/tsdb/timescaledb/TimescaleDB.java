package cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb;

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
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimescaleDB implements IDatabase {

  private Connection connection;
  private static String tableName;
  private static Config config;
  private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDB.class);
  private static final String convertToHypertable =
      "SELECT create_hypertable('%s', 'time', chunk_time_interval => 86400000);";
  private static final String dropTable = "DROP TABLE %s;";

  public TimescaleDB() {
    config = ConfigDescriptor.getInstance().getConfig();
    tableName = config.DB_NAME;
  }

  @Override
  public void init() throws TsdbException {
    try {
      Class.forName(Constants.POSTGRESQL_JDBC_NAME);
      connection = DriverManager.getConnection(
          String.format(Constants.POSTGRESQL_URL, config.host, config.port, config.DB_NAME),
          Constants.POSTGRESQL_USER,
          Constants.POSTGRESQL_PASSWD
      );
    } catch (ClassNotFoundException | SQLException e) {
      e.printStackTrace();
      LOGGER.error("Initialize InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    //delete old data
    Statement statement = null;
    try {
      statement = connection.createStatement();
      assert statement != null;
      statement.execute(String.format(dropTable, tableName));
      // wait for deletion complete
      LOGGER.info("Waiting {}ms for old data deletion.", config.INIT_WAIT_TIME);
      Thread.sleep(config.INIT_WAIT_TIME);
    } catch (SQLException | InterruptedException e) {
      LOGGER.warn("delete old data table {} failed, because: {}", tableName, e.getMessage());
      if (!e.getMessage().contains("does not exist")) {
        e.printStackTrace();
        throw new TsdbException(e);
      }
    } finally {
      try {
        assert statement != null;
        statement.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void close() throws TsdbException {
    if (connection == null) {
      return;
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error("Failed to close TimeScaleDB connection because: {}", e.getMessage());
      throw new TsdbException(e);
    }
  }

  /**
   * Map the data schema concepts as follow:
   * <ul>
   * <li>DB_NAME -> table name</li>
   * <li>storage group name -> a field in table</li>
   * <li>device name -> a field in table</li>
   * <li>sensors -> fields in table</li>
   * </ul>
   * <p> Reference link: https://docs.timescale.com/v1.0/getting-started/creating-hypertables</p>
   * -- We start by creating a regular SQL table
   * <p><code>
   * CREATE TABLE conditions ( time        TIMESTAMPTZ       NOT NULL, location    TEXT
   * NOT NULL, temperature DOUBLE PRECISION  NULL, humidity    DOUBLE PRECISION  NULL );
   * </code></p>
   * -- This creates a hypertable that is partitioned by time using the values in the `time` column.
   * <p><code>SELECT create_hypertable('conditions', 'time');</code></p>
   */
  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    Statement statement = null;
    try {
      statement = connection.createStatement();
      String pgsql = getCreateTableSql(tableName);
      statement.execute(pgsql);
      LOGGER.debug("CreateTableSQL Statement:  {}", pgsql);
      statement.execute(String.format(convertToHypertable, tableName));
      LOGGER.debug("convertToHypertable Statement:  {}",
          String.format(convertToHypertable, tableName));
    } catch (SQLException e) {
      LOGGER.error("Can't create PG table because: {}", e.getMessage());
      throw new TsdbException(e);
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        LOGGER.warn("Can't close statement when setting storage group because: {}", e.getMessage());
        throw new TsdbException(e);
      }
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    long st;
    long en;
    Statement statement = null;
    try {
      statement = connection.createStatement();
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
    } finally {
      if (statement != null) {
        try {
          statement.clearBatch();
          statement.close();
        } catch (SQLException e) {
          LOGGER.error("Can't close statement when insert one batch because: {}", e.getMessage());
        }
      }
    }
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') and time=1535558400000.
   *
   * @param preciseQuery universal precise query condition parameters
   */
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    StringBuilder builder = getSampleQuerySqlHead(preciseQuery.getDeviceSchema());
    builder.append(" AND time = ").append(preciseQuery.getTimestamp());
    return executeQueryAndGetStatus(builder.toString());
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') AND (time >= 1535558400000 AND
   * time <= 1535558650000).
   *
   * @param rangeQuery universal range query condition parameters
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    StringBuilder builder = getSampleQuerySqlHead(rangeQuery.getDeviceSchema());
    addWhereTimeClause(builder, rangeQuery);
    return executeQueryAndGetStatus(builder.toString());
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') and (s_2 > 78).
   *
   * @param valueRangeQuery contains universal range query with value filter parameters
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    StringBuilder builder = getSampleQuerySqlHead(valueRangeQuery.getDeviceSchema());
    addWhereValueClause(valueRangeQuery.getDeviceSchema(), builder,
        valueRangeQuery.getValueThreshold());
    return executeQueryAndGetStatus(builder.toString());
  }

  /**
   * eg. SELECT device, count(s_2) FROM tutorial WHERE (device='d_2') AND (time >= 1535558400000 and
   * time <= 1535558650000) GROUP BY device.
   *
   * @param aggRangeQuery contains universal aggregation query with time filter parameters
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    StringBuilder builder = getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(),
        aggRangeQuery.getAggFun());
    addWhereTimeClause(builder, aggRangeQuery);
    builder.append("GROUP BY device");
    return executeQueryAndGetStatus(builder.toString());
  }

  /**
   * eg. SELECT device, count(s_2) FROM tutorial WHERE (device='d_2') AND (s_2>10) GROUP BY device.
   *
   * @param aggValueQuery contains universal aggregation query with value filter parameters
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    StringBuilder builder = getAggQuerySqlHead(aggValueQuery.getDeviceSchema(),
        aggValueQuery.getAggFun());
    addWhereValueClause(aggValueQuery.getDeviceSchema(), builder,
        aggValueQuery.getValueThreshold());
    builder.append(" GROUP BY device");
    return executeQueryAndGetStatus(builder.toString());
  }

  /**
   * eg. SELECT device, count(s_2) FROM tutorial WHERE (device='d_2') AND (time >= 1535558400000 and
   * time <= 1535558650000) AND (s_2>10) GROUP BY device.
   *
   * @param aggRangeValueQuery contains universal aggregation query with time and value filters
   * parameters
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    StringBuilder builder = getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(),
        aggRangeValueQuery.getAggFun());
    addWhereTimeClause(builder, aggRangeValueQuery);
    addWhereValueClause(aggRangeValueQuery.getDeviceSchema(), builder,
        aggRangeValueQuery.getValueThreshold());
    builder.append("GROUP BY device");
    return executeQueryAndGetStatus(builder.toString());
  }

  /**
   * eg. SELECT time_bucket(5000, time) AS sampleTime, device, count(s_2) FROM tutorial WHERE
   * (device='d_2') AND (time >= 1535558400000 and time <= 1535558650000) GROUP BY time, device.
   *
   * @param groupByQuery contains universal group by query condition parameters
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    StringBuilder builder = getGroupByQuerySqlHead(groupByQuery.getDeviceSchema(),
        groupByQuery.getAggFun(), groupByQuery.getGranularity());
    addWhereTimeClause(builder, groupByQuery);
    builder.append(" GROUP BY time, device");
    return executeQueryAndGetStatus(builder.toString());
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') ORDER BY time DESC LIMIT 1. The
   * last and first commands do not use indexes, and instead perform a sequential scan through their
   * groups. They are primarily used for ordered selection within a GROUP BY aggregate, and not as
   * an alternative to an ORDER BY time DESC LIMIT 1 clause to find the latest value (which will use
   * indexes).
   *
   * @param latestPointQuery contains universal latest point query condition parameters
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    StringBuilder builder = getSampleQuerySqlHead(latestPointQuery.getDeviceSchema());
    builder.append("ORDER BY time DESC LIMIT 1");
    return executeQueryAndGetStatus(builder.toString());
  }

  private Status executeQueryAndGetStatus(String sql) {
    LOGGER.info("{} 提交执行的查询SQL: {}", Thread.currentThread().getName(), sql);
    long st;
    long en;
    int line = 0;
    int queryResultPointNum = 0;
    Statement statement = null;
    try {
      statement = connection.createStatement();
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
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          LOGGER.error("Can't close statement when execute one query because: {}", e.getMessage());
        }
      }
    }
  }

  /**
   * 创建查询语句--(带有聚合函数的查询) .
   * SELECT device, avg(cpu) FROM metrics WHERE (device='d_1' OR device='d_2')
   */
  private StringBuilder getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT device");
    addFunSensor(aggFun, builder, devices.get(0).getSensors());

    builder.append(" FROM ").append(tableName);
    addDeviceCondition(builder, devices);
    return builder;
  }

  /**
   * 创建查询语句--(带有GroupBy函数的查询) .
   * SELECT time_bucket(5, time) AS sampleTime, device, avg(cpu) FROM
   * metrics WHERE (device='d_1' OR device='d_2').
   */
  private StringBuilder getGroupByQuerySqlHead(List<DeviceSchema> devices, String aggFun,
      long timeUnit) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT time_bucket(").append(timeUnit).append(", time) AS sampleTime, device");

    addFunSensor(aggFun, builder, devices.get(0).getSensors());

    builder.append(" FROM ").append(tableName);
    addDeviceCondition(builder, devices);
    return builder;
  }

  /**
   * 创建查询语句--(不带有聚合函数的查询) .
   * SELECT time, device, cpu FROM metrics WHERE (device='d_1' OR device='d_2').
   */
  private StringBuilder getSampleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT time, device");
    addFunSensor(null, builder, devices.get(0).getSensors());

    builder.append(" FROM ").append(tableName);

    addDeviceCondition(builder, devices);
    return builder;
  }

  private void addFunSensor(String method, StringBuilder builder, List<String> list) {
    if (method != null) {
      list.forEach(sensor ->
          builder.append(", ").append(method).append("(").append(sensor).append(")")
      );
    } else {
      list.forEach(sensor -> builder.append(", ").append(sensor));
    }
  }

  private void addDeviceCondition(StringBuilder builder, List<DeviceSchema> devices) {
    builder = builder.append(" WHERE (");
    for (DeviceSchema deviceSchema : devices) {
      builder.append("device='").append(deviceSchema.getDevice()).append("'").append(" OR ");
    }
    builder.delete(builder.length() - 4, builder.length());
    builder.append(")");
  }

  /**
   * add time filter for query statements.
   *
   * @param builder sql header
   * @param rangeQuery range query
   */
  private static void addWhereTimeClause(StringBuilder builder, RangeQuery rangeQuery) {
    builder.append(" AND (time >= ").append(rangeQuery.getStartTimestamp());
    builder.append(" and time <= ").append(rangeQuery.getEndTimestamp()).append(") ");
  }

  /**
   * add value filter for query statements.
   *
   * @param devices query device schema
   * @param builder sql header
   * @param valueThreshold lower bound of query value filter
   * @return sql with value filter
   */
  private static void addWhereValueClause(List<DeviceSchema> devices, StringBuilder builder,
      double valueThreshold) {
    boolean first = true;
    for (String sensor : devices.get(0).getSensors()) {
      if (first) {
        builder.append(" AND (").append(sensor).append(" > ").append(valueThreshold);
        first = false;
      } else {
        builder.append(" and ").append(sensor).append(" > ").append(valueThreshold);
      }
    }
    builder.append(")");
  }

  /**
   * add group by clause for query.
   *
   * @param sqlHeader sql header
   * @param timeGranularity time granularity of group by
   */
  private static String addGroupByClause(String sqlHeader, long timeGranularity) {
    StringBuilder builder = new StringBuilder(sqlHeader);
    builder.append(" GROUP BY time(").append(timeGranularity).append("ms)");
    return builder.toString();
  }

  /**
   * -- Creating a regular SQL table example.
   * <p>
   * CREATE TABLE group_0 ( time       BIGINT              NOT NULL, sGroup     TEXT
   * NOT NULL, device     TEXT                NOT NULL, s_0        DOUBLE PRECISION    NULL, s_1
   * DOUBLE PRECISION    NULL );
   * </p>
   * @return create table SQL String
   */
  private String getCreateTableSql(String tableName) {
    StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
    sqlBuilder.append("time BIGINT NOT NULL, sGroup TEXT NOT NULL, device TEXT NOT NULL");
    for (String sensor : config.SENSOR_CODES) {
      sqlBuilder.append(", ").append(sensor).append(" ").append(config.DATA_TYPE)
          .append(" PRECISION NULL");
    }
    sqlBuilder.append(");");
    return sqlBuilder.toString();
  }

  /**
   * eg.
   * <p>
   * INSERT INTO conditions(time, group, device, s_0, s_1) VALUES (1535558400000, 'group_0', 'd_0',
   * 70.0, 50.0);
   * </p>
   */
  private String getInsertOneBatchSql(DeviceSchema deviceSchema, long timestamp,
      List<String> values) {
    StringBuilder builder = new StringBuilder();
    builder.append("insert into ")
        .append(tableName)
        .append("(time, sGroup, device");
    for (String sensor : deviceSchema.getSensors()) {
      builder.append(",").append(sensor);
    }
    builder.append(") values(");
    builder.append(timestamp);
    builder.append(",'").append(deviceSchema.getGroup()).append("'");
    builder.append(",'").append(deviceSchema.getDevice()).append("'");
    for (String value : values) {
      builder.append(",").append(value);
    }
    builder.append(")");
    LOGGER.debug("getInsertOneBatchSql: {}", builder);
    return builder.toString();
  }
}

