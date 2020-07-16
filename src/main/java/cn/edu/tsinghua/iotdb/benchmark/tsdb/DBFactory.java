package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.fakedb.FakeDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.influxdb.InfluxDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.DoubleIOTDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.IoTDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.IoTDBSession;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.kairosdb.KairosDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb.TimescaleDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.opentsdb.OpenTSDB;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBFactory.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  public IDatabase getDatabase() throws SQLException {
    switch (config.DB_SWITCH) {
      case Constants.DB_IOT:
        switch (config.INSERT_MODE) {
          case Constants.INSERT_USE_JDBC:
            return new IoTDB();
          case Constants.INSERT_USE_SESSION:
            return new IoTDBSession();
        }
      case Constants.DB_INFLUX:
        return new InfluxDB();
      case Constants.DB_KAIROS:
        return new KairosDB();
      case Constants.DB_TIMESCALE:
        return new TimescaleDB();
      case Constants.DB_FAKE:
        return new FakeDB();
      case Constants.DB_OPENTS:
        return new OpenTSDB();
      case Constants.DB_DOUBLE_IOT:
        return new DoubleIOTDB();
      default:
        LOGGER.error("unsupported database {}", config.DB_SWITCH);
        throw new SQLException("unsupported database " + config.DB_SWITCH);
    }
  }

}
