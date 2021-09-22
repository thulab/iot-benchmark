package cn.edu.tsinghua.iotdb.benchmark.piarchive;

import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.enums.Type;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

public class PIArchive implements IDatabase {
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

  private static final String driverClassName = "com.osisoft.jdbc.Driver";
  private static final Properties properties = new Properties();
  private static final String url = "jdbc:pioledb://%s/Data Source=%s; Integrated Security=SSPI";
  private static Connection connection;
  private static DBConfig dbConfig;

  public PIArchive(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  @Override
  public void init() throws TsdbException {
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
      System.err.println(ex);
    }
  }

  @Override
  public void cleanup() throws TsdbException {}

  @Override
  public void close() throws TsdbException {}

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    for (DeviceSchema deviceSchema : schemaList) {
      String deviceName = deviceSchema.getDevice();
      for (String sensor : deviceSchema.getSensors()) {
        String pointName = deviceName + "_" + sensor;
        String pointTypex = typeMap(baseDataSchema.getSensorType(deviceName, sensor));
      }
    }
//    // create time series
//    List<String> paths = new ArrayList<>();
//    List<TSDataType> tsDataTypes = new ArrayList<>();
//    List<TSEncoding> tsEncodings = new ArrayList<>();
//    List<CompressionType> compressionTypes = new ArrayList<>();
//    int count = 0;
//    int createSchemaBatchNum = 10000;
//    for (DeviceSchema deviceSchema : schemaList) {
//      int sensorIndex = 0;
//      for (String sensor : deviceSchema.getSensors()) {
//        paths.add(getSensorPath(deviceSchema, sensor));
//        Type datatype = baseDataSchema.getSensorType(deviceSchema.getDevice(), sensor);
//        tsDataTypes.add(Enum.valueOf(TSDataType.class, datatype.name));
//        tsEncodings.add(Enum.valueOf(TSEncoding.class, getEncodingType(datatype)));
//        // TODO remove when [IOTDB-1518] is solved(not supported null)
//        compressionTypes.add(Enum.valueOf(CompressionType.class, "SNAPPY"));
//        if (++count % createSchemaBatchNum == 0) {
//          registerTimeseriesBatch(metaSession, paths, tsEncodings, tsDataTypes, compressionTypes);
//        }
//        sensorIndex++;
//      }
//    }
//    if (!paths.isEmpty()) {
//      registerTimeseriesBatch(metaSession, paths, tsEncodings, tsDataTypes, compressionTypes);
//    }
  }

  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    return null;
  }

  @Override
  public Status insertOneSensorBatch(Batch batch) throws DBConnectException {
    return null;
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return null;
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    return null;
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

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return null;
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return null;
  }

  @Override
  public String typeMap(Type iotdbType) {
    switch (iotdbType) {
      case INT32:
        return "Int32";
      case INT64:
        return "Digital";
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
