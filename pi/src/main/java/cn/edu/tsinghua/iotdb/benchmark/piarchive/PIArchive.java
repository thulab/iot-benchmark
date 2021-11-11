package cn.edu.tsinghua.iotdb.benchmark.piarchive;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PIArchive implements IDatabase {
  private static final MetaDataSchema baseDataSchema = MetaDataSchema.getInstance();
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  private static final String driverClassName = "com.osisoft.jdbc.Driver";
  private static final Properties properties = new Properties();
  private static final String url = "jdbc:pioledb://%s/Data Source=%s; Integrated Security=SSPI";
  private Connection connection;
  private DBConfig dbConfig;

  private static final Logger LOGGER = LoggerFactory.getLogger(PIArchive.class);

  public PIArchive(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  @Override
  public void init() {
    properties.put("TrustedConnection", "yes");
    properties.put("ProtocolOrder", "nettcp:5462");
    properties.put("LogConsole", "True");
    properties.put("LogLevel", "2");

    try {
      Class.forName(driverClassName).newInstance();
      connection =
          DriverManager.getConnection(
              String.format(url, dbConfig.getHOST().get(0), "PI"), properties);
    } catch (Exception ex) {
      LOGGER.error(String.valueOf(ex));
    }
  }

  @Override
  public void cleanup() {
    try {
      PreparedStatement deleteRecordsStatement =
          connection.prepareStatement("delete piarchive..picomp2 where tag like 'g_%'");
      deleteRecordsStatement.execute();
      PreparedStatement deletePointsStatement =
          connection.prepareStatement("delete pipoint..classic where tag like 'g_%'");
      deletePointsStatement.execute();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }

  @Override
  public void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public boolean registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    if (!config.getOPERATION_PROPORTION().split(":")[0].equals("0")) {
      try {
        PreparedStatement pStatement =
            connection.prepareStatement(
                "INSERT pipoint..classic (tag, pointtypex, compressing, future) VALUES (?, ?, 0, 1)");
        for (DeviceSchema deviceSchema : schemaList) {
          String group = deviceSchema.getGroup();
          String deviceName = deviceSchema.getDevice();
          for (Sensor sensor : deviceSchema.getSensors()) {
            String pointName = group + "_" + deviceName + "_" + sensor.getName();
            String pointTypex = typeMap(sensor.getSensorType());
            pStatement.setString(1, pointName);
            pStatement.setString(2, pointTypex);
            pStatement.addBatch();
          }
        }
        pStatement.executeBatch();
        connection.commit();
        pStatement.clearBatch();
        pStatement.close();
      } catch (SQLException e) {
        e.printStackTrace();
        throw new TsdbException(e);
      }
    }
    return true;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try (PreparedStatement pStatement =
        connection.prepareStatement(
            "INSERT piarchive..picomp2 (tag, value, time) VALUES (?, ?, ?)")) {
      DeviceSchema deviceSchema = batch.getDeviceSchema();
      ArrayList<String> pointsName = new ArrayList<>();
      for (Sensor sensor : deviceSchema.getSensors()) {
        pointsName.add(deviceSchema.getGroup() + "_" + deviceSchema.getDevice() + "_" + sensor.getName());
      }
      for (Record record : batch.getRecords()) {
        for (int i = 0; i < record.getRecordDataValue().size(); i++) {
          pStatement.setString(1, pointsName.get(i));
          pStatement.setString(2, String.valueOf(record.getRecordDataValue().get(i)));
          pStatement.setString(3, formatter.format(record.getTimestamp()));
          pStatement.addBatch();
        }
      }
      pStatement.executeBatch();
      connection.commit();
      pStatement.clearBatch();
      pStatement.close();
      return new Status(true);
    } catch (SQLException e) {
      LOGGER.error(String.valueOf(e));
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    DeviceSchema deviceSchema = preciseQuery.getDeviceSchema().get(0);
    String tagName =
        deviceSchema.getGroup()
            + "_"
            + deviceSchema.getDevice()
            + "_"
            + deviceSchema.getSensors().get(0);
    String sql =
        String.format(
            "SELECT tag, value, time FROM PIARCHIVE..PICOMP2 WHERE tag = '%s' AND time = '%s'",
            tagName, formatter.format(preciseQuery.getTimestamp()));
    return query(sql);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    DeviceSchema deviceSchema = rangeQuery.getDeviceSchema().get(0);
    String tagName =
        deviceSchema.getGroup()
            + "_"
            + deviceSchema.getDevice()
            + "_"
            + deviceSchema.getSensors().get(0);
    String sql =
        String.format(
            "SELECT tag, value, time FROM PIARCHIVE..PICOMP2 WHERE tag = '%s' AND time >= '%s' AND time <= '%s'",
            tagName,
            formatter.format(rangeQuery.getStartTimestamp()),
            formatter.format(rangeQuery.getEndTimestamp()));
    return query(sql);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    DeviceSchema deviceSchema = valueRangeQuery.getDeviceSchema().get(0);
    String tagName =
        deviceSchema.getGroup()
            + "_"
            + deviceSchema.getDevice()
            + "_"
            + deviceSchema.getSensors().get(0);
    String sql =
        String.format(
            "SELECT tag, value, time FROM PIARCHIVE..PICOMP2 WHERE tag = '%s' AND time >= '%s' AND time <= '%s' AND value > %s",
            tagName,
            formatter.format(valueRangeQuery.getStartTimestamp()),
            formatter.format(valueRangeQuery.getEndTimestamp()),
            valueRangeQuery.getValueThreshold());
    return query(sql);
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    DeviceSchema deviceSchema = aggRangeQuery.getDeviceSchema().get(0);
    String tagName =
        deviceSchema.getGroup()
            + "_"
            + deviceSchema.getDevice()
            + "_"
            + deviceSchema.getSensors().get(0);
    String sql =
        String.format(
            "SELECT count(*) FROM PIARCHIVE..PICOMP2 WHERE tag = '%s' AND time >= '%s' AND time <= '%s'",
            tagName,
            formatter.format(aggRangeQuery.getStartTimestamp()),
            formatter.format(aggRangeQuery.getEndTimestamp()));
    return query(sql);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    DeviceSchema deviceSchema = aggValueQuery.getDeviceSchema().get(0);
    String tagName =
        deviceSchema.getGroup()
            + "_"
            + deviceSchema.getDevice()
            + "_"
            + deviceSchema.getSensors().get(0);
    String sql =
        String.format(
            "SELECT count(*) FROM PIARCHIVE..PICOMP2 WHERE tag = '%s' AND value > %s",
            tagName, aggValueQuery.getValueThreshold());
    return query(sql);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    DeviceSchema deviceSchema = aggRangeValueQuery.getDeviceSchema().get(0);
    String tagName =
        deviceSchema.getGroup()
            + "_"
            + deviceSchema.getDevice()
            + "_"
            + deviceSchema.getSensors().get(0);
    String sql =
        String.format(
            "SELECT count(*) FROM PIARCHIVE..PICOMP2 WHERE tag = '%s' AND time >= '%s' AND time <= '%s' AND value > %s",
            tagName,
            formatter.format(aggRangeValueQuery.getValueThreshold()),
            formatter.format(aggRangeValueQuery.getStartTimestamp()),
            aggRangeValueQuery.getEndTimestamp());
    return query(sql);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return null;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return null;
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    DeviceSchema deviceSchema = rangeQuery.getDeviceSchema().get(0);
    String tagName =
        deviceSchema.getGroup()
            + "_"
            + deviceSchema.getDevice()
            + "_"
            + deviceSchema.getSensors().get(0);
    String sql =
        String.format(
            "select tag, value, time from piarchive..picomp2 where tag = '%s' AND time >= '%s' AND time <= '%s' order by time desc",
            tagName,
            formatter.format(rangeQuery.getStartTimestamp()),
            formatter.format(rangeQuery.getEndTimestamp()));
    return query(sql);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    DeviceSchema deviceSchema = valueRangeQuery.getDeviceSchema().get(0);
    String tagName =
        deviceSchema.getGroup()
            + "_"
            + deviceSchema.getDevice()
            + "_"
            + deviceSchema.getSensors().get(0);
    String sql =
        String.format(
            "select tag, value, time from piarchive..picomp2 where tag = '%s' AND time >= '%s' AND time <= '%s' AND value > %s order by time desc",
            tagName,
            formatter.format(valueRangeQuery.getStartTimestamp()),
            formatter.format(valueRangeQuery.getEndTimestamp()),
            valueRangeQuery.getValueThreshold());
    return query(sql);
  }

  public Status query(String sql) {
    try {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(sql);
      int resultLineNum = 0;
      try {
        while (resultSet.next()) {
          resultLineNum++;
        }
      } catch (NoSuchMethodError e) {
      }
      int queryResultPointNum =
          resultLineNum * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM();
      return new Status(true, queryResultPointNum);
    } catch (SQLException throwables) {
      throwables.printStackTrace();
      return new Status(false, 0);
    }
  }

  @Override
  public String typeMap(SensorType iotdbType) {
    switch (iotdbType) {
      case INT32:
      case INT64:
        return "Int32";
      case FLOAT:
        return "Float32";
      case DOUBLE:
        return "Float64";
      case BOOLEAN:
      case TEXT:
      default:
        return "String";
    }
  }
}
