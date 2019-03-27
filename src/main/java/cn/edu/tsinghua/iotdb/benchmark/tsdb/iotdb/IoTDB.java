package cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
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
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
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
    sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  @Override
  public void init() throws TsdbException{
    try {
      Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
      connection = DriverManager
          .getConnection(String.format(Constants.URL, config.host, config.port), Constants.USER,
              Constants.PASSWD);
    } catch (Exception e) {
      LOGGER.error("Initialize IoTDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException{
    // currently no implementation
  }

  @Override
  public void close() throws TsdbException{
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
  public void registerSchema(Measurement measurement) throws TsdbException{
    DataSchema dataSchema = DataSchema.getInstance();
    int count = 0;
    // set storage group
    try {
      try (Statement statement = connection.createStatement()) {
        for (int i = 0; i < config.GROUP_NUMBER; i++) {
          statement.addBatch(
              String.format(SET_STORAGE_GROUP_SQL,
                  Constants.ROOT_SERIES_NAME + "." + DeviceSchema.GROUP_NAME_PREFIX + i));
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
      for (Entry<Integer, List<DeviceSchema>> entry : dataSchema.getClientBindSchema().entrySet()) {
        List<DeviceSchema> deviceSchemaList = entry.getValue();
        for (DeviceSchema deviceSchema : deviceSchemaList) {
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
      }
      statement.executeBatch();
      statement.clearBatch();
    } catch (SQLException e) {
      LOGGER.error("Register IoTDB schema failed because ", e);
      throw new TsdbException(e);
    }

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
    builder.append(" FROM ").append(devices.get(0).getDevicePath());
    for (int i = 1; i < devices.size(); i++) {
      builder.append(", ").append(devices.get(i).getDevicePath());
    }

    return builder.toString();
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
    }
  }

  private String getRangeQuerySql(RangeQuery rangeQuery) {
    String startTime = sdf.format(new Date(rangeQuery.getStartTimestamp()));
    String endTime = sdf.format(new Date(rangeQuery.getEndTimestamp()));
    return getSimpleQuerySqlHead(rangeQuery.getDeviceSchema()) + " WHERE time >= " + startTime
        + " AND time <= " + endTime;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    long st;
    long en;
    try (Statement statement = connection.createStatement()) {
      for (Entry<Long, List<String>> entry : batch.getRecords().entrySet()) {
        String sql = getInsertOneBatchSql(batch.getDeviceSchema(), entry.getKey(),
            entry.getValue());
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

}
