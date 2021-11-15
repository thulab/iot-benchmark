package cn.edu.tsinghua.iotdb.benchmark.iginx;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.LastQueryDataSet;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
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

import java.util.ArrayList;
import java.util.List;

public class IginX implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IginX.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final String DB_NAME;
  private final String FULL_PATHS;
  private final DBConfig dbConfig;
  private Session session;

  public IginX(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    this.DB_NAME = dbConfig.getDB_NAME() + ".cluster_" + config.getBENCHMARK_INDEX();
    this.FULL_PATHS = DB_NAME + ".*";
  }

  @Override
  public void init() throws TsdbException {
    session =
        new Session(
            dbConfig.getHOST().get(0),
            dbConfig.getPORT().get(0),
            dbConfig.getUSERNAME(),
            dbConfig.getPASSWORD());

    try {
      session.openSession();
    } catch (SessionException e) {
      LOGGER.error("Initialize IginX failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try {
      session.deleteColumn(FULL_PATHS);
      LOGGER.info("Finish clean data!");
    } catch (SessionException | ExecutionException e) {
      LOGGER.warn("Clear Data failed because ", e);
    }
  }

  @Override
  public void close() throws TsdbException {
    try {
      session.closeSession();
    } catch (SessionException e) {
      LOGGER.error("Failed to close IginX connection because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public boolean registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    return true;
  }

  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    List<String> paths = new ArrayList<>();
    List<DataType> types = new ArrayList<>();
    buildPathsAndTypesFromSchema(batch.getDeviceSchema(), paths, types);

    int size = batch.getRecords().size();
    long[] timestamps = new long[size];
    Object[] values = new Object[size];

    for (int i = 0; i < size; i++) {
      Record record = batch.getRecords().get(i);
      timestamps[i] = record.getTimestamp();

      int colSize = record.getRecordDataValue().size();
      Object[] rowValue = new Object[colSize];
      for (int j = 0; j < colSize; j++) {
        Object value = record.getRecordDataValue().get(j);
        if (value instanceof String) value = ((String) value).getBytes();
        rowValue[j] = value;
      }
      values[i] = rowValue;
    }

    try {
      session.insertRowRecords(paths, timestamps, values, types, null);
      return new Status(true);
    } catch (SessionException | ExecutionException e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    List<String> paths = new ArrayList<>();
    buildPathsFromSchemaList(preciseQuery.getDeviceSchema(), paths);
    return rangeQuery(
        "preciseQuery", paths, preciseQuery.getTimestamp(), preciseQuery.getTimestamp());
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    List<String> paths = new ArrayList<>();
    buildPathsFromSchemaList(rangeQuery.getDeviceSchema(), paths);
    return rangeQuery(
        "rangeQuery", paths, rangeQuery.getStartTimestamp(), rangeQuery.getEndTimestamp());
  }

  // query range [start, end]
  private Status rangeQuery(String queryType, List<String> paths, long startTime, long endTime) {
    StringBuilder builder = new StringBuilder();
    builder
        .append(queryType)
        .append("\n")
        .append("StartTime: ")
        .append(startTime)
        .append(", EndTime: ")
        .append(endTime)
        .append("\n");
    paths.forEach(path -> builder.append(path).append(" "));
    String queryDetails = builder.toString();

    int queryResultPointNum = 0;
    try {
      SessionQueryDataSet dataSet =
          session.queryData(
              paths, startTime, endTime + 1 // iginx range query [start, end)
              );
      int line = dataSet.getValues().size();
      queryResultPointNum = line * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM();
      return new Status(true, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, queryResultPointNum, e, queryDetails);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum, new Exception(t), queryDetails);
    }
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    List<String> paths = new ArrayList<>();
    buildPathsFromSchemaList(valueRangeQuery.getDeviceSchema(), paths);

    String booleanExpression =
        buildBooleanExpression(
            valueRangeQuery.getDeviceSchema(), (int) valueRangeQuery.getValueThreshold());

    StringBuilder builder = new StringBuilder();
    builder
        .append("valueRangeQuery")
        .append("\n")
        .append("StartTime: ")
        .append(valueRangeQuery.getStartTimestamp())
        .append(", EndTime: ")
        .append(valueRangeQuery.getEndTimestamp())
        .append("\n")
        .append("booleanExpression: ")
        .append(booleanExpression)
        .append("\n");
    paths.forEach(path -> builder.append(path).append(" "));
    String queryDetails = builder.toString();

    int queryResultPointNum = 0;
    try {
      SessionQueryDataSet dataSet =
          session.valueFilterQuery(
              paths,
              valueRangeQuery.getStartTimestamp(),
              valueRangeQuery.getEndTimestamp() + 1, // iginx range query [start, end)
              booleanExpression);
      int line = dataSet.getValues().size();
      queryResultPointNum = line * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM();
      return new Status(true, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, queryResultPointNum, e, queryDetails);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum, new Exception(t), queryDetails);
    }
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    // iginx session not support multi Func for now.
    return null;
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    // iginx session not support for now.
    return null;
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    // iginx session not support for now.
    return null;
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    // iginx session not support multi func for now.
    return null;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    List<String> paths = new ArrayList<>();
    buildPathsFromSchemaList(latestPointQuery.getDeviceSchema(), paths);

    StringBuilder builder = new StringBuilder();
    builder.append("latestPointQuery").append("\n");
    paths.forEach(path -> builder.append(path).append(" "));
    String queryDetails = builder.toString();

    int queryResultPointNum = 0;
    try {
      LastQueryDataSet dataSet = session.queryLast(paths, 0);
      queryResultPointNum = dataSet.getPoints().size();
      return new Status(true, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, queryResultPointNum, e, queryDetails);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum, new Exception(t), queryDetails);
    }
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    // iginx session not support for now.
    return null;
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    // iginx session not support for now.
    return null;
  }

  private void buildPathsAndTypesFromSchema(
      DeviceSchema schema, List<String> paths, List<DataType> types) {
    String group = schema.getGroup();
    String device = schema.getDevice();
    List<Sensor> sensors = schema.getSensors();
    sensors.forEach(
        sensor -> {
          paths.add(DB_NAME + "." + group + "." + device + "." + sensor.getName());
          types.add(sensorTypeToDataType(sensor.getSensorType()));
        });
  }

  private void buildPathsFromSchemaList(List<DeviceSchema> schemaList, List<String> paths) {
    schemaList.forEach(
        schema -> {
          String group = schema.getGroup();
          String device = schema.getDevice();
          List<Sensor> sensors = schema.getSensors();
          sensors.forEach(
              sensor -> paths.add(DB_NAME + "." + group + "." + device + "." + sensor.getName()));
        });
  }

  private String buildBooleanExpression(List<DeviceSchema> schemas, int valueThreshold) {
    StringBuilder builder = new StringBuilder();
    for (DeviceSchema schema : schemas) {
      String group = schema.getGroup();
      String device = schema.getDevice();

      if (!schema.getSensors().isEmpty()) {
        String sensorName = schema.getSensors().get(0).getName();
        builder
            .append(DB_NAME + "." + group + "." + device + "." + sensorName)
            .append(" > ")
            .append(valueThreshold);
        for (int i = 1; i < schema.getSensors().size(); i++) {
          sensorName = schema.getSensors().get(i).getName();
          builder
              .append(" AND ")
              .append(DB_NAME + "." + group + "." + device + "." + sensorName)
              .append(" > ")
              .append(valueThreshold);
        }
      }
    }
    return builder.toString();
  }

  private DataType sensorTypeToDataType(SensorType type) {
    switch (type) {
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INT32:
        return DataType.INTEGER;
      case INT64:
        return DataType.LONG;
      case TEXT:
        return DataType.BINARY;
      default:
        LOGGER.error("Unsupported data sensorType {}.", type);
        return null;
    }
  }
}
