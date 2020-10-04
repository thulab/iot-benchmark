package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;

import java.sql.SQLException;

public class DBFactory {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public DBFactory() { }

  public IDatabase getDatabase() throws SQLException {
    String dbClass = Constants.IOTDB011_CLASS;
    try {
      switch(config.getDB_SWITCH()) {
        case "IoTDB011":
          dbClass = Constants.IOTDB011_CLASS;
          break;
        case "IoTDB010":
          dbClass = Constants.IOTDB010_CLASS;
          break;
        case "IoTDB009":
          dbClass = Constants.IOTDB009_CLASS;
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
