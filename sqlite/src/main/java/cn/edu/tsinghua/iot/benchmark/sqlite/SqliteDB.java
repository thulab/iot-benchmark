package cn.edu.tsinghua.iot.benchmark.sqlite;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqliteDB implements IDatabase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqliteDB.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final String URL = "jdbc:sqlite:%s.db";

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
  private DBConfig dbConfig;

  public SqliteDB(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  @Override
  public void init() throws TsdbException {
    try {
      Class.forName("org.sqlite.JDBC");
      connection = DriverManager.getConnection(String.format(URL, dbConfig.getDB_NAME()));
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
      for (String sensorType : TYPES) {
        String tableName = dbConfig.getDB_NAME() + "_" + sensorType;
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
   * @return
   */
  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    long start;
    long end;
    start = System.nanoTime();
    try (Statement statement = connection.createStatement()) {
      for (String sensorType : TYPES) {
        String create =
            String.format(CREATE_TABLE, dbConfig.getDB_NAME(), sensorType, sensorType, sensorType);
        statement.execute(create);
      }
      LOGGER.info("Finish Register!");
      end = System.nanoTime();
    } catch (SQLException sqlException) {
      LOGGER.error(sqlException.getMessage());
      throw new TsdbException("Failed to register!", sqlException);
    }
    return TimeUtils.convertToSeconds(end - start, "ns");
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
                  idPredix,
                  i,
                  record.getTimestamp(),
                  values.get(i),
                  deviceSchema.getSensors().get(i).getSensorType()));
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

  private String getOneLine(
      long idPredix, int sensorIndex, long time, Object value, SensorType sensorType) {
    long sensorNow = sensorIndex + idPredix;
    String sysType = typeMap(sensorType);
    StringBuffer sql =
        new StringBuffer("INSERT INTO ")
            .append(dbConfig.getDB_NAME() + "_" + sysType)
            .append(" values (");
    sql.append(sensorNow).append(",");
    sql.append(time).append(",");
    if (sensorType == SensorType.BOOLEAN) {
      if ((boolean) value) {
        sql.append("1").append(")");
      } else {
        sql.append("0").append(")");
      }
    } else if (sensorType == SensorType.TEXT) {
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
    long groupNow = Long.parseLong(group.replace(config.getGROUP_NAME_PREFIX(), ""));
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
        for (String sensorType : TYPES) {
          String sql = getHeader(idPrefix, deviceSchema.getSensors(), sensorType);
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
        for (String sensorType : TYPES) {
          String sql = getHeader(idPrefix, deviceSchema.getSensors(), sensorType);
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
        for (String sensorType : VALUE_TYPES) {
          String sql = getHeader(idPrefix, deviceSchema.getSensors(), sensorType);
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
        for (String sensorType : types) {
          String sql =
              getHeader(aggRangeQuery.getAggFun(), deviceSchema.getSensors(), idPrefix, sensorType);
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
        for (String sensorType : VALUE_TYPES) {
          String sql =
              getHeader(aggValueQuery.getAggFun(), deviceSchema.getSensors(), idPrefix, sensorType);
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
        for (String sensorType : VALUE_TYPES) {
          String sql =
              getHeader(
                  aggRangeValueQuery.getAggFun(), deviceSchema.getSensors(), idPrefix, sensorType);
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
        for (String sensorType : VALUE_TYPES) {
          String sql = getHeader("max", deviceSchema.getSensors(), idPrefix, sensorType);
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
        for (Sensor sensor : deviceSchema.getSensors()) {
          long sensorId = idPrefix + Integer.parseInt(sensor.getName().split("_")[1]);
          search.add(String.valueOf(sensorId));
        }
        String ids = String.join(",", search);
        for (String sensorType : TYPES) {
          String sql =
              "select * from "
                  + dbConfig.getDB_NAME()
                  + "_"
                  + sensorType
                  + ", (select max(pk_TimeStamp) as target from "
                  + dbConfig.getDB_NAME()
                  + "_"
                  + sensorType
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
        for (String sensorType : TYPES) {
          String sql = getHeader(idPrefix, deviceSchema.getSensors(), sensorType);
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
        for (String sensorType : VALUE_TYPES) {
          String sql = getHeader(idPrefix, deviceSchema.getSensors(), sensorType);
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

  private String getHeader(long device, List<Sensor> sensors, String sysType) {
    List<String> search = new ArrayList<>();
    for (Sensor sensor : sensors) {
      long sensorId = device + Integer.parseInt(sensor.getName().split("_")[1]);
      search.add(String.valueOf(sensorId));
    }

    StringBuilder stringBuilder = new StringBuilder("SELECT * from ");
    stringBuilder.append(dbConfig.getDB_NAME()).append("_").append(sysType);
    stringBuilder.append(" where pk_fk_Id in (").append(String.join(",", search)).append(")");
    return stringBuilder.toString();
  }

  private String getHeader(String aggFun, List<Sensor> sensors, long device, String sysType) {
    List<String> search = new ArrayList<>();
    for (Sensor sensor : sensors) {
      long sensorId = device + Integer.parseInt(sensor.getName().split("_")[1]);
      search.add(String.valueOf(sensorId));
    }
    String target = "value";
    if (aggFun.startsWith("count")) {
      target = "*";
    }

    StringBuilder stringBuilder =
        new StringBuilder("SELECT ").append(aggFun).append("(").append(target).append(") from ");
    stringBuilder.append(dbConfig.getDB_NAME()).append("_").append(sysType);
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
   * map the given sensorType string name to the name in the target DB
   *
   * @param iotdbSensorType : "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"
   * @return
   */
  @Override
  public String typeMap(SensorType iotdbSensorType) {
    switch (iotdbSensorType) {
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
        LOGGER.error("Error Type: " + iotdbSensorType);
        return "TEXT";
    }
  }
}
