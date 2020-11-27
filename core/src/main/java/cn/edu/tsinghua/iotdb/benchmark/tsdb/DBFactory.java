package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;

import java.sql.SQLException;

public class DBFactory {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public DBFactory() { }

  public IDatabase getDatabase() throws SQLException {
    String dbClass;
    try {
      switch(config.getDB_SWITCH()) {
        case Constants.TDP_IOTDB:
          switch(config.getVERSION()) {
            case "0.11.0":
              if(config.getINSERT_MODE().equals(Constants.INSERT_USE_JDBC)) {
                dbClass = Constants.IOTDB011_JDBC_CLASS;
              } else {
                dbClass = Constants.IOTDB011_SESSION_CLASS;
              }
              break;
            case "0.10.0":
              if(config.getINSERT_MODE().equals(Constants.INSERT_USE_JDBC)) {
                dbClass = Constants.IOTDB010_JDBC_CLASS;
              } else {
                dbClass = Constants.IOTDB010_SESSION_CLASS;
              }
              break;
            case "0.9.0":
              if(config.getINSERT_MODE().equals(Constants.INSERT_USE_JDBC)) {
                dbClass = Constants.IOTDB009_JDBC_CLASS;
              } else {
                dbClass = Constants.IOTDB009_SESSION_CLASS;
              }
              break;
            default:
              throw new SQLException("didn't support this database");
          }
          break;
        case Constants.DB_INFLUX:
          dbClass = Constants.INFLUXDB_CLASS;
          break;
        case Constants.DB_KAIROS:
          dbClass = Constants.KAIROSDB_CLASS;
          break;
        case Constants.DB_OPENTS:
          dbClass = Constants.OPENTSDB_CLASS;
          break;
        case Constants.DB_TIMESCALE:
          dbClass = Constants.TIMESCALEDB_CLASS;
          break;
        case Constants.DB_TAOSDB:
          dbClass = Constants.TAOSDB_CLASS;
          break;
        default:
          throw new SQLException("didn't support this database");
      }
      return (IDatabase) Class.forName(dbClass).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    throw new SQLException("init database " + config.getDB_SWITCH() + " failed");
  }
}
