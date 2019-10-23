package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.mysql;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlRecorder implements ITestDataPersistence {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(MySqlRecorder.class);
  private static final String SAVE_CONFIG = "insert into CONFIG values(NULL, %s, %s, %s)";
  private static final String SAVE_RESULT = "insert into FINAL_RESULT values(NULL, '%s', '%s', '%s', '%s')";
  private Connection mysqlConnection = null;
  private Config config = ConfigDescriptor.getInstance().getConfig();
  private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");
  private String localName;
  private String day;
  private static final long EXP_TIME = System.currentTimeMillis();
  private String projectID = String.format("%s_%s_%s_%s",config.BENCHMARK_WORK_MODE, config.DB_SWITCH, config.REMARK, sdf.format(new java.util.Date(EXP_TIME)));
  private Statement statement;
  private long count = 0;

  public MySqlRecorder() {
    try {
      InetAddress localhost = InetAddress.getLocalHost();
      localName = localhost.getHostName();
    } catch (UnknownHostException e) {
      localName = "localName";
      LOGGER.error("获取本机主机名称失败;UnknownHostException：{}", e.getMessage(), e);
    }
    localName = localName.replace("-", "_");
    localName = localName.replace(".", "_");
    Date date = new Date(EXP_TIME);
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
    day = dateFormat.format(date);
    try {
      Class.forName(Constants.MYSQL_DRIVENAME);
      mysqlConnection = DriverManager.getConnection(config.MYSQL_URL);
      initTable();
      statement = mysqlConnection.createStatement();
    } catch (SQLException e) {
      LOGGER.error("mysql 初始化失败，原因是", e);
    } catch (ClassNotFoundException e) {
      LOGGER.error("mysql 连接初始化失败，原因是", e);
    }

  }

  // 检查记录本次实验的表格是否已经创建，没有则创建
  private void initTable() {
    Statement stat = null;
    try {
      stat = mysqlConnection.createStatement();
      if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_SERVER_MODE)
          || config.BENCHMARK_WORK_MODE.equals(Constants.MODE_CLIENT_SYSTEM_INFO)) {
        if (!hasTable("SERVER_MODE_" + localName + "_" + day)) {
          stat.executeUpdate("create table SERVER_MODE_"
              + localName
              + "_"
              + day
              + "(id BIGINT, "
              + "cpu_usage DOUBLE,mem_usage DOUBLE,diskIo_usage DOUBLE,net_recv_rate DOUBLE,net_send_rate DOUBLE, pro_mem_size DOUBLE, "
              + "dataFileSize DOUBLE,infoFizeSize DOUBLE,metadataFileSize DOUBLE,OverflowFileSize DOUBLE, deltaFileSize DOUBLE, walFileSize DOUBLE,"
              + "tps DOUBLE,MB_read DOUBLE,MB_wrtn DOUBLE,"
              + "totalFileNum INT, dataFileNum INT, socketNum INT, settledNum INT, infoNum INT,"
              + "schemaNum INT, metadataNum INT, overflowNum INT, walNum INT, "
              + "primary key(id))");
          LOGGER.info("Table SERVER_MODE create success!");
        }
        return;
      }
      if (!hasTable("CONFIG")) {
        stat.executeUpdate(
            "create table CONFIG (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, projectID VARCHAR(150), configuration_item VARCHAR(150), configuration_value VARCHAR(150))AUTO_INCREMENT = 1;");
        LOGGER.info("Table CONFIG create success!");
      }
      if (!hasTable("FINAL_RESULT")) {
        stat.executeUpdate(
            "create table FINAL_RESULT (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, projectID VARCHAR(150), operation VARCHAR(50), result_key VARCHAR(150), result_value VARCHAR(150))AUTO_INCREMENT = 1;");
        LOGGER.info("Table FINAL_RESULT create success!");
      }
      if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_TEST_WITH_DEFAULT_PATH) && !hasTable(
          projectID)) {
        stat.executeUpdate("create table "
            + projectID
            + "(id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, recordTime varchar(50), clientName varchar(50), "
            + "operation varchar(50), okPoint INTEGER, failPoint INTEGER, latency DOUBLE, rate DOUBLE, remark varchar(1000))AUTO_INCREMENT = 1;");
        LOGGER.info("Table {} create success!", projectID);
      }
    } catch (SQLException e) {
      LOGGER.error("mysql 创建表格失败,原因是", e);
    } finally {
      try {
        if (stat != null) {
          stat.close();
        }
      } catch (SQLException e) {
        LOGGER.error("close statement failed", e);
      }
    }
  }

  @Override
  public void insertSystemMetrics(double cpu, double mem, double io, double networkReceive, double networkSend,
      double processMemSize,
      double dataSize, double systemSize, double sequenceSize, double overflowSize, double walSize,
      float tps, float ioRead, float ioWrite, List<Integer> openFileList) {

    Statement stat = null;
    String sql = "";
    try {
      stat = mysqlConnection.createStatement();
      sql = String.format("insert into SERVER_MODE_" + localName
              + "_" + day + " values(%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%d,%d,%d,%d,%d,%d,%d,%d,%d)",
          System.currentTimeMillis(),
          cpu,
          mem,
          io,
          networkReceive,
          networkSend,
          processMemSize,
          dataSize,
          systemSize,
          sequenceSize,
          overflowSize,
          walSize,
          tps,
          ioRead,
          ioWrite,
          openFileList.get(0),
          openFileList.get(1),
          openFileList.get(2),
          openFileList.get(3),
          openFileList.get(4),
          openFileList.get(5),
          openFileList.get(6),
          openFileList.get(7),
          openFileList.get(8));
      stat.executeUpdate(sql);
    } catch (SQLException e) {
      LOGGER.error("{} insert into mysql failed", sql, e);
    } finally {
      if (stat != null) {
        try {
          stat.close();
        } catch (SQLException e) {
          LOGGER.error("close statement failed", e);
        }
      }
    }
  }

  // 将插入测试的以batch为单位的中间结果存入数据库
  @Override
  public void saveOperationResult(String operation, int okPoint, int failPoint,
      double latency, String remark) {

    double rate = 0;
    if (latency > 0) {
      rate = okPoint * 1000 / latency; //unit: points/second
    }
    String time = df.format(new java.util.Date(System.currentTimeMillis()));
    String mysqlSql = String
        .format("insert into %s values(NULL,'%s','%s','%s',%d,%d,%f,%f,'%s')", projectID,
            time, Thread.currentThread().getName(), operation, okPoint, failPoint, latency, rate,
            remark);
    try {
      statement = mysqlConnection.createStatement();
      statement.execute(mysqlSql);
      count ++;
      if(count % 5000 == 0) {
        statement.executeBatch();
        statement.clearBatch();
      }
    } catch (Exception e) {
      LOGGER.error("Exception: {}", e.getMessage(), e);
      try {
        if (!mysqlConnection.isValid(100)) {
          LOGGER.info("Try to reconnect to MySQL");
          try {
            Class.forName(Constants.MYSQL_DRIVENAME);
            mysqlConnection = DriverManager.getConnection(config.MYSQL_URL);
          } catch (Exception ex) {
            LOGGER.error("Reconnect to MySQL failed because", ex);
          }
        }
      } catch (SQLException ex) {
        LOGGER.error("Test if MySQL connection is valid failed", ex);
      }
      LOGGER.error(
          "{} save saveInsertProcess info into mysql failed! Error：{}",
          Thread.currentThread().getName(), e.getMessage());
      LOGGER.error("{}", mysqlSql);
    }

  }

  // 存储实验结果
  @Override
  public void saveResult(String operation, String k, String v) {
    Statement stat = null;
    String sql = String.format(SAVE_RESULT, projectID, operation, k, v);
    try {
      stat = mysqlConnection.createStatement();
      stat.executeUpdate(sql);
    } catch (SQLException e) {
      LOGGER.error("{}将结果信息写入mysql失败，because ：{}", sql, e);
    } finally {
      if (stat != null) {
        try {
          stat.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close statement for saving result because", e);
        }
      }
    }
  }

  @Override
  public void saveTestConfig() {
    Statement stat = null;
    String sql = "";
    try {
      stat = mysqlConnection.createStatement();
      if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'MODE'", "'DEFAULT_TEST_MODE'");
        stat.addBatch(sql);
      }
      switch (config.DB_SWITCH.trim()) {
        case Constants.DB_IOT:
        case Constants.DB_TIMESCALE:
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'ServerIP'", "'" + config.HOST + "'");
          stat.addBatch(sql);
          break;
        case Constants.DB_INFLUX:
        case Constants.DB_OPENTS:
        case Constants.DB_KAIROS:
        case Constants.DB_CTS:
          String TSHost = config.DB_URL
              .substring(config.DB_URL.lastIndexOf('/') + 1, config.DB_URL.lastIndexOf(':'));
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'ServerIP'", "'" + TSHost + "'");
          stat.addBatch(sql);
          break;
        default:
          throw new SQLException("unsupported database " + config.DB_SWITCH);
      }
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
          "'CLIENT'", "'" + localName + "'");
      stat.addBatch(sql);
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
          "'DB_SWITCH'", "'" + config.DB_SWITCH + "'");
      stat.addBatch(sql);
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
          "'VERSION'", "'" + config.VERSION + "'");
      stat.addBatch(sql);
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
          "'CLIENT_NUMBER'", "'" + config.CLIENT_NUMBER + "'");
      stat.addBatch(sql);
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'", "'LOOP'",
          "'" + config.LOOP + "'");
      stat.addBatch(sql);
      if (config.BENCHMARK_WORK_MODE
          .equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'查询数据集存储组数'",
            "'" + config.GROUP_NUMBER + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'查询数据集设备数'", "'" + config.DEVICE_NUMBER
                + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'查询数据集传感器数'", "'" + config.SENSOR_NUMBER
                + "'");
        stat.addBatch(sql);
        if (config.DB_SWITCH.equals(Constants.DB_IOT)) {
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'IOTDB编码方式'", "'" + config.ENCODING + "'");
          stat.addBatch(sql);
        }

        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'QUERY_DEVICE_NUM'", "'" + config.QUERY_DEVICE_NUM
                + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'QUERY_SENSOR_NUM'", "'" + config.QUERY_SENSOR_NUM
                + "'");
        stat.addBatch(sql);

        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'IS_OVERFLOW'", "'" + config.IS_OVERFLOW + "'");
        stat.addBatch(sql);
        if (config.IS_OVERFLOW) {
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'OVERFLOW_RATIO'", "'" + config.OVERFLOW_RATIO + "'");
          stat.addBatch(sql);
        }
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'MUL_DEV_BATCH'", "'" + config.MUL_DEV_BATCH + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'DEVICE_NUMBER'", "'" + config.DEVICE_NUMBER + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'GROUP_NUMBER'", "'" + config.GROUP_NUMBER + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'DEVICE_NUMBER'", "'" + config.DEVICE_NUMBER + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'SENSOR_NUMBER'", "'" + config.SENSOR_NUMBER + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'BATCH_SIZE'", "'" + config.BATCH_SIZE + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'POINT_STEP'", "'" + config.POINT_STEP + "'");
        stat.addBatch(sql);
        if (config.DB_SWITCH.equals(Constants.DB_IOT)) {
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'ENCODING'", "'" + config.ENCODING + "'");
          stat.addBatch(sql);
        }
      }
      stat.executeBatch();
    } catch (SQLException e) {
      LOGGER.error("{}将配置信息写入mysql失败，because ：{}", sql, e);
    } finally {
      if (stat != null) {
        try {
          stat.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close statement for config info because", e);
        }
      }
    }
  }

  @Override
  public void close() {
    if (mysqlConnection != null) {
      try {
        statement.executeBatch();
        LOGGER.info("execute preparedStatement, count = {}", count);
        statement.close();
        mysqlConnection.close();
      } catch (SQLException e) {
        LOGGER.error("mysql 连接关闭失败,原因是:", e);
      }
    }
  }

  /**
   * 数据库中是否已经存在名字为table的表
   */
  private Boolean hasTable(String table) throws SQLException {
    String showTableTemplate = "show tables like \"%s\"";
    String checkTable = String.format(showTableTemplate, table);
    try (Statement stmt = mysqlConnection.createStatement()) {
      try (ResultSet resultSet = stmt.executeQuery(checkTable)) {
        return resultSet.next();
      }
    }
  }

}
