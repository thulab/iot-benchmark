package cn.edu.tsinghua.iotdb.benchmark.mssqlserver;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MsSQLServerDB implements IDatabase {
  private static final Logger LOGGER = LoggerFactory.getLogger(MsSQLServerDB.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String DBDRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  public Connection connection = null;

  private static final String CREATE_TABLE =
      "CREATE TABLE ["
          + "%s_%s]\n"
          + "([pk_fk_Id] [bigint] NOT NULL,\n"
          + "[pk_TimeStamp] [datetime2](7) NOT NULL,\n"
          + "[Value] [%s] NULL,\n"
          + "CONSTRAINT PK_test_%s PRIMARY KEY CLUSTERED\n"
          + "([pk_fk_Id] ASC,\n"
          + "[pk_TimeStamp] ASC\n"
          + ") WITH (IGNORE_DUP_KEY = ON) ON [PRIMARY]\n"
          + ")ON [PRIMARY]\n"
          + "With (DATA_COMPRESSION = %s)";
  private static final String INSERT_SQL = "Insert into %s values(?,?,?)";
  private static final String[] SELECT_SQL = {
    "SELECT * from %s where pk_fk_Id in (?) and pk_TimeStamp = ?",
    "SELECT * from %s where pk_fk_Id in (?) and pk_TimeStamp >= ? and pk_TimeStamp <= ?",
    "SELECT * from %s where pk_fk_Id in (?) and pk_TimeStamp >= ? and pk_TimeStamp <= ? and value > ?",
    "SELECT %s(%s) from %s where pk_fk_Id in (?) and pk_TimeStamp >= ? and pk_TimeStamp <= ?",
    "SELECT %s(%s) from %s where pk_fk_Id in (?) and value > ?",
    "SELECT %s(%s) from %s where pk_fk_Id in (?) and pk_TimeStamp >= ? and pk_TimeStamp <= ? and value > ?",
    "SELECT max(value) from %s where pk_fk_Id in (?) and pk_TimeStamp >= ? and pk_TimeStamp <= ? group by datediff(ss,'1970-01-01', pk_TimeStamp) / %s",
    "SELECT * from %s, (select max(pk_TimeStamp) as target from %s where pk_fk_Id in (?)) as m where pk_fk_Id in (?) and pk_TimeStamp=m.target",
    "SELECT * from %s where pk_fk_Id in (?) and pk_TimeStamp >= ? and pk_TimeStamp <= ? order by pk_TimeStamp desc",
    "SELECT * from %s where pk_fk_Id in (?) and pk_TimeStamp >= ? and pk_TimeStamp <= ? and value > ? order by pk_TimeStamp desc",
  };

  private PreparedStatement[] insertStatements = new PreparedStatement[6];
  // first: sensorType second: query index
  private PreparedStatement[][] queryStatements = new PreparedStatement[6][10];

  private static final String DELETE_TABLE = "drop table if exists %s_%s";
  private DBConfig dbConfig;

  public MsSQLServerDB(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  @Override
  public void init() throws TsdbException {
    try {
      Class.forName(DBDRIVER);
      connection =
          DriverManager.getConnection(
              "jdbc:sqlserver://"
                  + dbConfig.getHOST().get(0)
                  + ":"
                  + dbConfig.getPORT().get(0)
                  + ";DataBaseName="
                  + dbConfig.getDB_NAME(),
              dbConfig.getUSERNAME(),
              dbConfig.getPASSWORD());

      // init preparedStatement
      for (SensorType sensorType : SensorType.values()) {
        String db = dbConfig.getDB_NAME() + "_" + typeMap(sensorType);
        String insertSql = String.format(INSERT_SQL, db);
        insertStatements[sensorType.ordinal()] = connection.prepareStatement(insertSql);
        for (int i = 0; i < SELECT_SQL.length; i++) {
          String query;
          if (i == 3 || i == 4 || i == 5) {
            String target = "value";
            if (config.getQUERY_AGGREGATE_FUN().startsWith("count")) {
              target = "*";
            }
            query = String.format(SELECT_SQL[i], config.getQUERY_AGGREGATE_FUN(), target, db);
          } else if (i == 6) {
            query = String.format(SELECT_SQL[i], db, config.getGROUP_BY_TIME_UNIT());
          } else if (i == 7) {
            query = String.format(SELECT_SQL[i], db, db);
          } else {
            query = String.format(SELECT_SQL[i], db);
          }
          queryStatements[sensorType.ordinal()][i] = connection.prepareStatement(query);
        }
      }
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
      for (SensorType sensorType : SensorType.values()) {
        statement.execute(String.format(DELETE_TABLE, dbConfig.getDB_NAME(), typeMap(sensorType)));
      }
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
   * @return
   */
  @Override
  public boolean registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    try {
      Statement statement = connection.createStatement();
      for (SensorType sensorType : SensorType.values()) {
        if (sensorType == SensorType.DOUBLE) {
          continue;
        }
        String sysType = typeMap(sensorType);
        String createSQL =
            String.format(
                CREATE_TABLE,
                dbConfig.getDB_NAME(),
                sysType,
                sysType,
                sysType,
                config.getCOMPRESSION());
        statement.execute(createSQL);
      }
      statement.close();
    } catch (SQLException sqlException) {
      LOGGER.warn("Failed to register", sqlException);
      throw new TsdbException(sqlException);
    }
    return true;
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
      for (Record record : batch.getRecords()) {
        List<Object> values = record.getRecordDataValue();
        for (int i = 0; i < values.size(); i++) {
          addBatch(
              idPredix,
              i,
              record.getTimestamp(),
              values.get(i),
              deviceSchema.getDevice(),
              deviceSchema.getSensors());
        }
      }
      for (PreparedStatement preparedStatement : insertStatements) {
        preparedStatement.executeBatch();
        preparedStatement.clearParameters();
      }
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
      PreparedStatement statement = connection.prepareStatement(INSERT_SQL);
      for (Record record : batch.getRecords()) {
        List<Object> values = record.getRecordDataValue();
        addBatch(
            idPredix,
            batch.getColIndex(),
            record.getTimestamp(),
            values.get(0),
            deviceSchema.getDevice(),
            deviceSchema.getSensors());
      }
      for (PreparedStatement preparedStatement : insertStatements) {
        preparedStatement.executeBatch();
        preparedStatement.clearParameters();
      }
      statement.close();
      return new Status(true);
    } catch (SQLException e) {
      e.printStackTrace();
      LOGGER.error("Write batch failed");
      return new Status(false, 0, e, e.getMessage());
    }
  }

  /**
   * Add Data into batch
   *
   * @param idPredix
   * @param sensorIndex
   * @param time
   * @param value
   * @param device
   * @param sensors
   * @throws SQLException
   */
  private void addBatch(
      long idPredix, int sensorIndex, long time, Object value, String device, List<Sensor> sensors)
      throws SQLException {
    long sensorNow = sensorIndex + idPredix;
    SensorType sensorType = sensors.get(sensorIndex).getSensorType();
    String valueStr = "";
    switch (sensorType) {
      case BOOLEAN:
        if ((boolean) value) {
          valueStr = "1";
        } else {
          valueStr = "0";
        }
        break;
      case TEXT:
        valueStr = "'" + value + "'";
        break;
      default:
        valueStr = String.valueOf(value);
        break;
    }
    PreparedStatement statement = insertStatements[sensorType.ordinal()];
    statement.setLong(1, sensorNow);
    statement.setTimestamp(2, new Timestamp(time));
    statement.setString(3, valueStr);
    statement.addBatch();
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
    long groupNow = Long.parseLong(group.replace(Constants.GROUP_NAME_PREFIX, ""));
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
    Timestamp timestamp = new Timestamp(preciseQuery.getTimestamp());
    try {
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (SensorType sensorType : SensorType.values()) {
          PreparedStatement statement = queryStatements[sensorType.ordinal()][0];
          statement.setString(1, getTargetDevices(idPrefix, deviceSchema.getSensors()));
          statement.setTimestamp(2, timestamp);
          ResultSet resultSet = statement.executeQuery();
          while (resultSet.next()) {
            result++;
          }
        }
      }
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
    Timestamp startTime = new Timestamp(rangeQuery.getStartTimestamp());
    Timestamp endTime = new Timestamp(rangeQuery.getEndTimestamp());
    try {
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (SensorType sensorType : SensorType.values()) {
          PreparedStatement statement = queryStatements[sensorType.ordinal()][1];
          statement.setString(1, getTargetDevices(idPrefix, deviceSchema.getSensors()));
          statement.setTimestamp(2, startTime);
          statement.setTimestamp(3, endTime);
          ResultSet resultSet = statement.executeQuery();
          while (resultSet.next()) {
            result++;
          }
        }
      }
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("rangeQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    List<DeviceSchema> deviceSchemas = valueRangeQuery.getDeviceSchema();
    Timestamp startTime = new Timestamp(valueRangeQuery.getStartTimestamp());
    Timestamp endTime = new Timestamp(valueRangeQuery.getEndTimestamp());
    try {
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (SensorType sensorType : SensorType.getValueTypes()) {
          PreparedStatement statement = queryStatements[sensorType.ordinal()][2];
          statement.setString(1, getTargetDevices(idPrefix, deviceSchema.getSensors()));
          statement.setTimestamp(2, startTime);
          statement.setTimestamp(3, endTime);
          statement.setDouble(4, valueRangeQuery.getValueThreshold());
          ResultSet resultSet = statement.executeQuery();
          while (resultSet.next()) {
            result++;
          }
        }
      }
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("valueRangeQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    List<DeviceSchema> deviceSchemas = aggRangeQuery.getDeviceSchema();
    Timestamp startTime = new Timestamp(aggRangeQuery.getStartTimestamp());
    Timestamp endTime = new Timestamp(aggRangeQuery.getEndTimestamp());
    try {
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        SensorType[] sensorTypes = SensorType.getValueTypes();
        if (aggRangeQuery.getAggFun().startsWith("count")) {
          sensorTypes = SensorType.values();
        }
        for (SensorType sensorType : sensorTypes) {
          PreparedStatement statement = queryStatements[sensorType.ordinal()][3];
          statement.setString(1, getTargetDevices(idPrefix, deviceSchema.getSensors()));
          statement.setTimestamp(2, startTime);
          statement.setTimestamp(3, endTime);
          ResultSet resultSet = statement.executeQuery();
          while (resultSet.next()) {
            result++;
          }
        }
      }
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("aggRangeQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    List<DeviceSchema> deviceSchemas = aggValueQuery.getDeviceSchema();
    try {
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (SensorType sensorType : SensorType.getValueTypes()) {
          PreparedStatement statement = queryStatements[sensorType.ordinal()][4];
          statement.setString(1, getTargetDevices(idPrefix, deviceSchema.getSensors()));
          statement.setDouble(2, aggValueQuery.getValueThreshold());
          ResultSet resultSet = statement.executeQuery();
          while (resultSet.next()) {
            result++;
          }
        }
      }
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("aggValueQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    List<DeviceSchema> deviceSchemas = aggRangeValueQuery.getDeviceSchema();
    Timestamp startTime = new Timestamp(aggRangeValueQuery.getStartTimestamp());
    Timestamp endTime = new Timestamp(aggRangeValueQuery.getEndTimestamp());
    try {
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (SensorType sensorType : SensorType.getValueTypes()) {
          PreparedStatement statement = queryStatements[sensorType.ordinal()][5];
          statement.setString(1, getTargetDevices(idPrefix, deviceSchema.getSensors()));
          statement.setTimestamp(2, startTime);
          statement.setTimestamp(3, endTime);
          statement.setDouble(4, aggRangeValueQuery.getValueThreshold());
          ResultSet resultSet = statement.executeQuery();
          while (resultSet.next()) {
            result++;
          }
        }
      }
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("aggRangeValueQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    List<DeviceSchema> deviceSchemas = groupByQuery.getDeviceSchema();
    Timestamp startTime = new Timestamp(groupByQuery.getStartTimestamp());
    Timestamp endTime = new Timestamp(groupByQuery.getEndTimestamp());
    try {
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (SensorType sensorType : SensorType.getValueTypes()) {
          PreparedStatement statement = queryStatements[sensorType.ordinal()][6];
          statement.setString(1, getTargetDevices(idPrefix, deviceSchema.getSensors()));
          statement.setTimestamp(2, startTime);
          statement.setTimestamp(3, endTime);
          ResultSet resultSet = statement.executeQuery();
          while (resultSet.next()) {
            result++;
          }
        }
      }
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("groupByQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    List<DeviceSchema> deviceSchemas = latestPointQuery.getDeviceSchema();
    try {
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        String ids = getTargetDevices(idPrefix, deviceSchema.getSensors());
        for (SensorType sensorType : SensorType.values()) {
          PreparedStatement statement = queryStatements[sensorType.ordinal()][7];
          statement.setString(1, ids);
          statement.setString(2, ids);
          ResultSet resultSet = statement.executeQuery();
          while (resultSet.next()) {
            result++;
          }
        }
      }
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("latestPointQuery Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    List<DeviceSchema> deviceSchemas = rangeQuery.getDeviceSchema();
    Timestamp startTime = new Timestamp(rangeQuery.getStartTimestamp());
    Timestamp endTime = new Timestamp(rangeQuery.getEndTimestamp());
    try {
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (SensorType sensorType : SensorType.values()) {
          PreparedStatement statement = queryStatements[sensorType.ordinal()][8];
          statement.setString(1, getTargetDevices(idPrefix, deviceSchema.getSensors()));
          statement.setTimestamp(2, startTime);
          statement.setTimestamp(3, endTime);
          ResultSet resultSet = statement.executeQuery();
          while (resultSet.next()) {
            result++;
          }
        }
      }
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("rangeQueryOrderByDesc Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    List<DeviceSchema> deviceSchemas = valueRangeQuery.getDeviceSchema();
    Timestamp startTime = new Timestamp(valueRangeQuery.getStartTimestamp());
    Timestamp endTime = new Timestamp(valueRangeQuery.getEndTimestamp());
    try {
      int result = 0;
      for (DeviceSchema deviceSchema : deviceSchemas) {
        long idPrefix = getId(deviceSchema.getGroup(), deviceSchema.getDevice(), null);
        for (SensorType sensorType : SensorType.getValueTypes()) {
          PreparedStatement statement = queryStatements[sensorType.ordinal()][9];
          statement.setString(1, getTargetDevices(idPrefix, deviceSchema.getSensors()));
          statement.setTimestamp(2, startTime);
          statement.setTimestamp(3, endTime);
          statement.setDouble(4, valueRangeQuery.getValueThreshold());
          ResultSet resultSet = statement.executeQuery();
          while (resultSet.next()) {
            result++;
          }
        }
      }
      return new Status(true, result);
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
      LOGGER.error("valueRangeQueryOrderByDesc Error!");
      return new Status(false, 0, sqlException, sqlException.getMessage());
    }
  }

  private String getTargetDevices(long device, List<Sensor> sensors) {
    List<String> search = new ArrayList<>();
    for (Sensor sensor : sensors) {
      long sensorId = device + Integer.parseInt(sensor.getName().split("_")[1]);
      search.add(String.valueOf(sensorId));
    }
    return String.join(",", search);
  }

  private String getHeader(long device, List<String> sensors, String sysType) {
    List<String> search = new ArrayList<>();
    for (String sensor : sensors) {
      long sensorId = device + Integer.parseInt(sensor.split("_")[1]);
      search.add(String.valueOf(sensorId));
    }

    StringBuilder stringBuilder = new StringBuilder("SELECT * from ");
    stringBuilder.append(dbConfig.getDB_NAME()).append("_").append(sysType);
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
    stringBuilder.append(dbConfig.getDB_NAME()).append("_").append(sysType);
    stringBuilder.append(" where pk_fk_Id in (").append(String.join(",", search)).append(")");
    return stringBuilder.toString();
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
        return "bit";
      case INT32:
        return "int";
      case INT64:
        return "bigint";
      case FLOAT:
      case DOUBLE:
        return "float";
      case TEXT:
        return "text";
      default:
        LOGGER.error(
            "Unsupported data sensorType {}, use default data sensorType: BINARY.",
            iotdbSensorType);
        return "text";
    }
  }
}
