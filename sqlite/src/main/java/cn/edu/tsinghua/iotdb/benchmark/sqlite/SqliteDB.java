package cn.edu.tsinghua.iotdb.benchmark.sqlite;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.schema.enums.Type;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqliteDB implements IDatabase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqliteDB.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();
  private static final String URL = "jdbc:sqlite:" + config.getDB_NAME() + ".db";

  private static final List<String> TYPES =
      new ArrayList<>(Arrays.asList("INTEGER", "REAL", "TEXT"));
  private static final List<String> VALUE_TYPES = TYPES.subList(0, 2);

  private static final String CREATE_TABLE =
      "CREATE TABLE "
          + "%s_%s\n"
          + "(pk_fk_Id INTEGER NOT NULL,\n"
          + "pk_TimeStamp INTEGER NOT NULL,\n"
          + "Value %s NULL,\n"
          + "CONSTRAINT PK_test_%s PRIMARY KEY (pk_fk_Id, pk_TimeStamp)\n"
          + ")";

  private Connection connection;
  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  @Override
  public void init() throws TsdbException {
    try {
      Class.forName("org.sqlite.JDBC");
      connection = DriverManager.getConnection(URL);
    } catch (Exception e) {
      LOGGER.error(e.getClass().getName() + ": " + e.getMessage());
      throw new TsdbException("Failed to init: ", e);
    }
  }

  /**
   * Cleanup any state for this DB, including the old data deletion. Called once before each test if
   * IS_DELETE_DATA=true.
   */
  @Override
  public void cleanup() throws TsdbException {
    try (Statement statement = connection.createStatement()) {
      for (String type : TYPES) {
        String tableName = config.getDB_NAME() + "_" + type;
        statement.execute("DROP TABLE IF EXISTS " + tableName);
      }
      LOGGER.info("Finish Clean up!");
    } catch (SQLException sqlException) {
      LOGGER.error("Failed to Clean up!");
      throw new TsdbException("Failed to clean up!", sqlException);
    }
  }

  /** Close the DB instance connections. Called once per DB instance. */
  @Override
  public void close() throws TsdbException {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException sqlException) {
        LOGGER.error(sqlException.getMessage());
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
    try (Statement statement = connection.createStatement()) {
      for (String type : TYPES) {
        String create = String.format(CREATE_TABLE, config.getDB_NAME(), type, type, type);
        statement.execute(create);
      }
      LOGGER.info("Finish Register!");
    } catch (SQLException sqlException) {
      LOGGER.error(sqlException.getMessage());
      throw new TsdbException("Failed to register!", sqlException);
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
        List<Object> values = record.getRecordDataValue();
        for (int i = 0; i < values.size(); i++) {
          statement.addBatch(
              getOneLine(
                  idPredix, i, record.getTimestamp(), values.get(i), deviceSchema.getDevice()));
        }
      }
      statement.executeBatch();
      statement.close();
      return new Status(true);
    } catch (SQLException e) {
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
        List<Object> values = record.getRecordDataValue();
        statement.addBatch(
            getOneLine(
                idPredix,
                batch.getColIndex(),
                record.getTimestamp(),
                values.get(batch.getColIndex()),
                deviceSchema.getDevice()));
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

  private String getOneLine(
      long idPredix, int sensorIndex, long time, Object value, String device) {
    long sensorNow = sensorIndex + idPredix;
    Type type = baseDataSchema.getSensorType(device, MetaUtil.getSensorName(sensorIndex));
    String sysType = typeMap(type);
    StringBuffer sql =
        new StringBuffer("INSERT INTO ")
            .append(config.getDB_NAME() + "_" + sysType)
            .append(" values (");
    sql.append(sensorNow).append(",");
    sql.append(time).append(",");
    if (type.equals("BOOLEAN")) {
      if ((boolean) value) {
        sql.append("1").append(")");
      } else {
        sql.append("0").append(")");
      }
    } else if (type.equals("TEXT")) {
      sql.append("'").append(value).append("')");
    } else {
      sql.append(value).append(")");
    }
    return sql.toString();
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
    long time = preciseQuery.getTimestamp();
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (String type : TYPES) {
          String sql = getHeader(idPrefix, deviceSchema.getSensors(), type);
          sql = addTimeClause(sql, time);
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            result++;
          }
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
    long startTime = rangeQuery.getStartTimestamp();
    long endTime = rangeQuery.getEndTimestamp();
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (String type : TYPES) {
          String sql = getHeader(idPrefix, deviceSchema.getSensors(), type);
          sql = addTimeClause(sql, startTime, endTime);
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            result++;
          }
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
    long startTime = valueRangeQuery.getStartTimestamp();
    long endTime = valueRangeQuery.getEndTimestamp();
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (String type : VALUE_TYPES) {
          String sql = getHeader(idPrefix, deviceSchema.getSensors(), type);
          sql = addTimeClause(sql, startTime, endTime);
          sql = addValueClause(sql, valueRangeQuery.getValueThreshold());
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            result++;
          }
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
    long startTime = aggRangeQuery.getStartTimestamp();
    long endTime = aggRangeQuery.getEndTimestamp();
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        List<String> types = VALUE_TYPES;
        if (aggRangeQuery.getAggFun().startsWith("count")) {
          types = TYPES;
        }
        for (String type : types) {
          String sql =
              getHeader(aggRangeQuery.getAggFun(), deviceSchema.getSensors(), idPrefix, type);
          sql = addTimeClause(sql, startTime, endTime);
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            result++;
          }
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
        for (String type : VALUE_TYPES) {
          String sql =
              getHeader(aggValueQuery.getAggFun(), deviceSchema.getSensors(), idPrefix, type);
          sql = addValueClause(sql, aggValueQuery.getValueThreshold());
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            result++;
          }
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
    long startTime = aggRangeValueQuery.getStartTimestamp();
    long endTime = aggRangeValueQuery.getEndTimestamp();
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (String type : VALUE_TYPES) {
          String sql =
              getHeader(aggRangeValueQuery.getAggFun(), deviceSchema.getSensors(), idPrefix, type);
          sql = addTimeClause(sql, startTime, endTime);
          sql = addValueClause(sql, aggRangeValueQuery.getValueThreshold());
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            result++;
          }
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
    long startTime = groupByQuery.getStartTimestamp();
    long endTime = groupByQuery.getEndTimestamp();
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (String type : VALUE_TYPES) {
          String sql = getHeader("max", deviceSchema.getSensors(), idPrefix, type);
          sql = addTimeClause(sql, startTime, endTime);
          sql = addGroupByClause(sql, groupByQuery.getGranularity());
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            result++;
          }
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
        List<String> search = new ArrayList<>();
        for (String sensor : deviceSchema.getSensors()) {
          long sensorId = idPrefix + Integer.parseInt(sensor.split("_")[1]);
          search.add(String.valueOf(sensorId));
        }
        String ids = String.join(",", search);
        for (String type : TYPES) {
          String sql =
              "select * from "
                  + config.getDB_NAME()
                  + "_"
                  + type
                  + ", (select max(pk_TimeStamp) as target from "
                  + config.getDB_NAME()
                  + "_"
                  + type
                  + " where pk_fk_Id in ("
                  + ids
                  + ")) as m"
                  + " where pk_fk_Id in ( "
                  + ids
                  + ") and pk_TimeStamp = m.target";
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            result++;
          }
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
    long startTime = rangeQuery.getStartTimestamp();
    long endTime = rangeQuery.getEndTimestamp();
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (String type : TYPES) {
          String sql = getHeader(idPrefix, deviceSchema.getSensors(), type);
          sql = addTimeClause(sql, startTime, endTime);
          sql = addOrderClause(sql);
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            result++;
          }
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
    long startTime = valueRangeQuery.getStartTimestamp();
    long endTime = valueRangeQuery.getEndTimestamp();
    try {
      Statement statement = connection.createStatement();
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (String type : VALUE_TYPES) {
          String sql = getHeader(idPrefix, deviceSchema.getSensors(), type);
          sql = addTimeClause(sql, startTime, endTime);
          sql = addValueClause(sql, valueRangeQuery.getValueThreshold());
          sql = addOrderClause(sql);
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            result++;
          }
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

  private String getHeader(long device, List<String> sensors, String sysType) {
    List<String> search = new ArrayList<>();
    for (String sensor : sensors) {
      long sensorId = device + Integer.parseInt(sensor.split("_")[1]);
      search.add(String.valueOf(sensorId));
    }

    StringBuilder stringBuilder = new StringBuilder("SELECT * from ");
    stringBuilder.append(config.getDB_NAME()).append("_").append(sysType);
    stringBuilder.append(" where pk_fk_Id in (").append(String.join(",", search)).append(")");
    return stringBuilder.toString();
  }

  private String getHeader(String aggFun, List<String> sensors, long device, String sysType) {
    List<String> search = new ArrayList<>();
    for (String sensor : sensors) {
      long sensorId = device + Integer.parseInt(sensor.split("_")[1]);
      search.add(String.valueOf(sensorId));
    }
    String target = "value";
    if (aggFun.startsWith("count")) {
      target = "*";
    }

    StringBuilder stringBuilder =
        new StringBuilder("SELECT ").append(aggFun).append("(").append(target).append(") from ");
    stringBuilder.append(config.getDB_NAME()).append("_").append(sysType);
    stringBuilder.append(" where pk_fk_Id in (").append(String.join(",", search)).append(")");
    return stringBuilder.toString();
  }

  private String addTimeClause(String sql, long time) {
    return sql + " and pk_TimeStamp = " + time;
  }

  private String addTimeClause(String sql, long startTime, long endTime) {
    return sql + " and pk_TimeStamp >= " + startTime + " and pk_TimeStamp <= " + endTime;
  }

  private String addValueClause(String sql, double value) {
    return sql + " and value > " + value;
  }

  private String addGroupByClause(String sql, long granularity) {
    return sql + " group by pk_TimeStamp / " + granularity;
  }

  private String addOrderClause(String sql) {
    return sql + " order by pk_TimeStamp desc";
  }

  /**
   * map the given type string name to the name in the target DB
   *
   * @param iotdbType : "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"
   * @return
   */
  @Override
  public String typeMap(Type iotdbType) {
    switch (iotdbType) {
      case BOOLEAN:
      case INT32:
      case INT64:
        return "INTEGER";
      case FLOAT:
      case DOUBLE:
        return "REAL";
      case TEXT:
        return "TEXT";
      default:
        LOGGER.error("Error Type: " + iotdbType);
        return "TEXT";
    }
  }
}
