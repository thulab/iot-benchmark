package cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static final String CREATE_SERIES_SQL =
      "CREATE TIMESERIES %s WITH DATATYPE=%s,ENCODING=%s,COMPRESSOR=%s";
  private static final String SET_STORAGE_GROUP_SQL = "SET STORAGE GROUP TO %s";
  private Connection connection;

  public IoTDB() {
    try {
      Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
      connection = DriverManager
          .getConnection(String.format(Constants.URL, config.host, config.port), Constants.USER,
              Constants.PASSWD);
    } catch (Exception e) {
      LOGGER.error("Initialize IoTDB failed because ", e);
    }
  }

  @Override
  public void init() {

  }

  @Override
  public void cleanup() {

  }

  @Override
  public void close() {

  }

  @Override
  public void registerSchema(Measurement measurement) {
    DataSchema dataSchema = DataSchema.getInstance();
    int count = 0;

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
      LOGGER.error("");
    }

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
    }


  }

  @Override
  public void insertOneBatch(Batch batch, Measurement measurement) {

  }

  @Override
  public void preciseQuery(PreciseQuery preciseQuery, Measurement measurement) {

  }

  @Override
  public void rangeQuery(RangeQuery rangeQuery, Measurement measurement) {

  }

  @Override
  public void valueRangeQuery(ValueRangeQuery valueRangeQuery, Measurement measurement) {

  }

  @Override
  public void aggRangeQuery(AggRangeQuery aggRangeQuery, Measurement measurement) {

  }

  @Override
  public void aggValueQuery(AggValueQuery aggValueQuery, Measurement measurement) {

  }

  @Override
  public void aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery, Measurement measurement) {

  }

  @Override
  public void groupByQuery(GroupByQuery groupByQuery, Measurement measurement) {

  }

  @Override
  public void latestPointQuery(LatestPointQuery latestPointQuery, Measurement measurement) {

  }
}
