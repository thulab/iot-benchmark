package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;

import java.sql.SQLException;

public class DBFactory {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public DBFactory() { }

  public IDatabase getDatabase() throws SQLException {
    try {
      return (IDatabase) Class.forName("cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDB").newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    throw new SQLException("init database " + config.getDB_SWITCH() + " failed");
  }
}
