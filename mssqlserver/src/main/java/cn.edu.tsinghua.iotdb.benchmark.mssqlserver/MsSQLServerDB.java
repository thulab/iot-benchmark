package cn.edu.tsinghua.iotdb.benchmark.mssqlserver;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;

/** @Author stormbroken Create by 2021/08/04 @Version 1.0 */
public class MsSQLServerDB implements IDatabase {
  private static final Logger LOGGER = LoggerFactory.getLogger(MsSQLServerDB.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String DBDRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  private static final String DBURL =
      "jdbc:sqlserver://"
          + config.getHOST().get(0)
          + ":"
          + config.getPORT().get(0)
          + ";DataBaseName="
          + config.getDB_NAME();
  // TODO remove after fix/core merge
  private static final String DBUSER = "test";
  private static final String DBPASSWORD = "12345678";
  private static final SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

  // 数据库连接
  public Connection connection = null;

  private static final String CREATE_TABLE =
      "CREATE TABLE ["
          + config.getDB_NAME()
          + "]\n"
          + "([pk_fk_Id] [bigint] NOT NULL,\n"
          + "[pk_TimeStamp] [datetime2](7) NOT NULL,\n"
          + "[Value] [float] NULL,\n"
          + "CONSTRAINT PK_test PRIMARY KEY CLUSTERED\n"
          + "([pk_fk_Id] ASC,\n"
          + "[pk_TimeStamp] ASC\n"
          + ") WITH (IGNORE_DUP_KEY = ON) ON [PRIMARY]\n"
          + ")ON [PRIMARY]";

  private static final String DELETE_TABLE = "drop table " + config.getDB_NAME();
  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  @Override
  public void init() throws TsdbException {
    try {
      Class.forName(DBDRIVER);
      connection = DriverManager.getConnection(DBURL, DBUSER, DBPASSWORD);
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.warn("Connect Error!");
      throw new TsdbException("Connect Error!", e);
    }
  }

  /**
   * Cleanup any state for this DB, including the old data deletion. Called once before each test if
   * IS_DELETE_DATA=true.
   */
  @Override
  public void cleanup() throws TsdbException {
    try {
      Statement statement = connection.createStatement();
      statement.execute(DELETE_TABLE);
      statement.close();
    } catch (SQLException sqlException) {
      LOGGER.warn("No need to clean!");
      throw new TsdbException("No need to clean!", sqlException);
    }
  }

  /** Close the DB instance connections. Called once per DB instance. */
  @Override
  public void close() throws TsdbException {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException sqlException) {
        throw new TsdbException("Failed to close", sqlException);
      }
    }
  }

  /**
   * Called once before each test if CREATE_SCHEMA=true.
   *
   * @param schemaList schema of devices to register
   */
  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    try {
      Statement statement = connection.createStatement();
      statement.execute(CREATE_TABLE);
      statement.close();
    } catch (SQLException sqlException) {
      LOGGER.warn("Failed to register", sqlException);
    }
  }

  /**
   * Insert one batch into the database, the DB implementation needs to resolve the data in batch
   * which contains device schema and Map[Long, List[String]] records. The key of records is a
   * timestamp and the value is a list of sensor value data.
   *
   * @param batch universal insertion data structure
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    long idPredix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
    try {
      Statement statement = connection.createStatement();
      for (Record record : batch.getRecords()) {
        String time = format.format(record.getTimestamp());
        List<Object> values = record.getRecordDataValue();
        for (int i = 0; i < values.size(); i++) {
          long sensorNow = i + idPredix;
          StringBuffer sql =
              new StringBuffer("INSERT INTO ").append(config.getDB_NAME()).append(" values (");
          sql.append(sensorNow).append(",");
          sql.append("'").append(time).append("',");
          sql.append(values.get(i)).append(")");
          statement.addBatch(sql.toString());
        }
      }
      statement.executeBatch();
      statement.close();
      return new Status(true);
    } catch (SQLException e) {
      e.printStackTrace();
      LOGGER.error("Write batch failed");
      return new Status(false, 0, e, e.getMessage());
    }
  }

  /**
   * Insert single-sensor one batch into the database, the DB implementation needs to resolve the
   * data in batch which contains device schema and Map[Long, List[String]] records. The key of
   * records is a timestamp and the value is one sensor value data.
   *
   * @param batch universal insertion data structure
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status insertOneSensorBatch(Batch batch) throws DBConnectException {
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    long idPredix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
    try {
      Statement statement = connection.createStatement();
      for (Record record : batch.getRecords()) {
        String time = format.format(record.getTimestamp());
        List<Object> values = record.getRecordDataValue();
        long sensorNow = batch.getColIndex() + idPredix;
        StringBuffer sql =
            new StringBuffer("INSERT INTO ").append(config.getDB_NAME()).append(" values (");
        sql.append(sensorNow).append(",");
        sql.append("'").append(time).append("',");
        sql.append(values.get(batch.getColIndex())).append(")");
        statement.addBatch(sql.toString());
      }
      statement.executeBatch();
      statement.close();
      return new Status(true);
    } catch (SQLException e) {
      e.printStackTrace();
      LOGGER.error("Write batch failed");
      return new Status(false, 0, e, e.getMessage());
    }
  }

  /**
   * 获取标识Id
   *
   * @param group
   * @param device
   * @param sensor
   * @return
   */
  private long getId(String group, String device, String sensor) {
    long groupNow = Long.parseLong(group.replace(config.getDB_NAME(), ""));
    long deviceNow = Long.parseLong(device.split("_")[1]);
    long sensorNow = 0;
    if (sensor != null) {
      sensorNow = Long.parseLong(sensor.split("_")[1]);
    }
    return config.getSENSOR_NUMBER()
            * config.getDEVICE_NUMBER()
            * (deviceNow + config.getGROUP_NUMBER() * groupNow)
        + sensorNow;
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    List<DeviceSchema> deviceSchemas = preciseQuery.getDeviceSchema();
    String time = format.format(preciseQuery.getTimestamp());
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String sql = getHeader(idPrefix);
        sql = addTimeClause(sql, time);
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
          result++;
        }
      }
      statement.close();
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("preciseQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    List<DeviceSchema> deviceSchemas = rangeQuery.getDeviceSchema();
    String startTime = format.format(rangeQuery.getStartTimestamp());
    String endTime = format.format(rangeQuery.getEndTimestamp());
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String sql = getHeader(idPrefix);
        sql = addTimeClause(sql, startTime, endTime);
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
          result++;
        }
      }
      statement.close();
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("preciseQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    List<DeviceSchema> deviceSchemas = valueRangeQuery.getDeviceSchema();
    String startTime = format.format(valueRangeQuery.getStartTimestamp());
    String endTime = format.format(valueRangeQuery.getEndTimestamp());
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String sql = getHeader(idPrefix);
        sql = addTimeClause(sql, startTime, endTime);
        sql = addValueClause(sql, valueRangeQuery.getValueThreshold());
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
          result++;
        }
      }
      statement.close();
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("preciseQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    List<DeviceSchema> deviceSchemas = aggRangeQuery.getDeviceSchema();
    String startTime = format.format(aggRangeQuery.getStartTimestamp());
    String endTime = format.format(aggRangeQuery.getEndTimestamp());
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String sql = getHeader(aggRangeQuery.getAggFun(), idPrefix);
        sql = addTimeClause(sql, startTime, endTime);
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
          result++;
        }
      }
      statement.close();
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("preciseQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    List<DeviceSchema> deviceSchemas = aggValueQuery.getDeviceSchema();
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String sql = getHeader(aggValueQuery.getAggFun(), idPrefix);
        sql = addValueClause(sql, aggValueQuery.getValueThreshold());
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
          result++;
        }
      }
      statement.close();
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("preciseQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    List<DeviceSchema> deviceSchemas = aggRangeValueQuery.getDeviceSchema();
    String startTime = format.format(aggRangeValueQuery.getStartTimestamp());
    String endTime = format.format(aggRangeValueQuery.getEndTimestamp());
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String sql = getHeader(aggRangeValueQuery.getAggFun(), idPrefix);
        sql = addTimeClause(sql, startTime, endTime);
        sql = addValueClause(sql, aggRangeValueQuery.getValueThreshold());
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
          result++;
        }
      }
      statement.close();
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("preciseQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    List<DeviceSchema> deviceSchemas = groupByQuery.getDeviceSchema();
    String startTime = format.format(groupByQuery.getStartTimestamp());
    String endTime = format.format(groupByQuery.getEndTimestamp());
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String sql = getHeader("max", idPrefix);
        sql = addTimeClause(sql, startTime, endTime);
        sql = addGroupByClause(sql, groupByQuery.getGranularity());
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
          result++;
        }
      }
      statement.close();
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("preciseQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    List<DeviceSchema> deviceSchemas = latestPointQuery.getDeviceSchema();
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String sql =
            "select * from "
                + config.getDB_NAME()
                + ", (select max(pk_TimeStamp) as target from ms where pk_fk_Id / "
                + config.getSENSOR_NUMBER()
                + " = "
                + idPrefix
                + ") as m"
                + " where pk_fk_Id / "
                + config.getSENSOR_NUMBER()
                + " = "
                + idPrefix
                + " and pk_TimeStamp = m.target";
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
          result++;
        }
      }
      statement.close();
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("preciseQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    List<DeviceSchema> deviceSchemas = rangeQuery.getDeviceSchema();
    String startTime = format.format(rangeQuery.getStartTimestamp());
    String endTime = format.format(rangeQuery.getEndTimestamp());
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String sql = getHeader(idPrefix);
        sql = addTimeClause(sql, startTime, endTime);
        sql = addOrderClause(sql);
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
          result++;
        }
      }
      statement.close();
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("preciseQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    List<DeviceSchema> deviceSchemas = valueRangeQuery.getDeviceSchema();
    String startTime = format.format(valueRangeQuery.getStartTimestamp());
    String endTime = format.format(valueRangeQuery.getEndTimestamp());
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String sql = getHeader(idPrefix);
        sql = addTimeClause(sql, startTime, endTime);
        sql = addValueClause(sql, valueRangeQuery.getValueThreshold());
        sql = addOrderClause(sql);
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
          result++;
        }
      }
      statement.close();
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("preciseQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  private String getHeader(long device) {
    return "SELECT * from "
        + config.getDB_NAME()
        + " where pk_fk_Id/"
        + config.getSENSOR_NUMBER()
        + "="
        + device;
  }

  private String getHeader(String aggFun, long device) {
    return "SELECT "
        + aggFun
        + "(value) from "
        + config.getDB_NAME()
        + " where pk_fk_Id/"
        + config.getSENSOR_NUMBER()
        + "="
        + device;
  }

  private String addTimeClause(String sql, String time) {
    return sql + " and pk_TimeStamp = '" + time + "'";
  }

  private String addTimeClause(String sql, String startTime, String endTime) {
    return sql + " and pk_TimeStamp >= '" + startTime + "' and pk_TimeStamp <= '" + endTime + "'";
  }

  private String addValueClause(String sql, double value) {
    return sql + " and value > " + value;
  }

  private String addGroupByClause(String sql, long granularity) {
    return sql + " group by datediff(ss,'1970-01-01', pk_TimeStamp) /  " + granularity;
  }

  private String addOrderClause(String sql) {
    return sql + " order by pk_TimeStamp desc";
  }
}
