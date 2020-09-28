package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBFactory.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private Properties dbMapping = new Properties();

  public DBFactory() {
    try {
      dbMapping.load(getClass().getResourceAsStream("dbmap.properties"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public IDatabase getDatabase() throws SQLException {
    String iotdbClassName = dbMapping.getProperty(config.DB_SWITCH);
    if(iotdbClassName == null) {
      LOGGER.error("unsupported database {}", config.DB_SWITCH);
      throw new SQLException("unsupported database " + config.DB_SWITCH);
    }
    try {
      return (IDatabase) Class.forName(iotdbClassName).newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    throw new SQLException("init database " + config.DB_SWITCH + " failed");
  }

}
