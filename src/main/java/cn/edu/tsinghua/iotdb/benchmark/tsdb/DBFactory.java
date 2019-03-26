package cn.edu.tsinghua.iotdb.benchmark.tsdb;


import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.influxdb.InfluxDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.IoTDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotengine.IoTDBEngine;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBFactory.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  public IDatabase getDatabase() throws SQLException {

    switch (config.DB_SWITCH) {
      case Constants.DB_IOT:
        return new IoTDB();
      case Constants.DB_INFLUX:
        return new InfluxDB();
      case Constants.DB_IOTENGINE:
        return new IoTDBEngine();
      default:
        LOGGER.error("unsupported database {}", config.DB_SWITCH);
        throw new SQLException("unsupported database " + config.DB_SWITCH);
    }
  }

}
