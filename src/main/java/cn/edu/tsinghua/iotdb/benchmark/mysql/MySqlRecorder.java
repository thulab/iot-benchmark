package cn.edu.tsinghua.iotdb.benchmark.mysql;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
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

public class MySqlRecorder {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(MySqlRecorder.class);
  private static final String SAVE_CONFIG = "insert into CONFIG values(NULL, %s, %s, %s)";
  private static final String SAVE_RESULT = "insert into FINAL_RESULT values(NULL, '%s', '%s', '%s', '%s')";
  private Connection mysqlConnection = null;
  private Config config = ConfigDescriptor.getInstance().getConfig();
  private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private String localName;
  private String day;
  private String projectID;
  private static final long EXP_TIME = System.currentTimeMillis();

  public MySqlRecorder() {
    if (config.IS_USE_MYSQL) {
      try {
        InetAddress localhost = InetAddress.getLocalHost();
        localName = localhost.getHostName();
      } catch (UnknownHostException e) {
        localName = "localName";
        LOGGER.error("获取本机主机名称失败;UnknownHostException：{}", e.getMessage(), e);
      }
      localName = localName.replace("-", "_");
      localName = localName.replace(".", "_");
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");
      String labID = sdf.format(new java.util.Date(EXP_TIME));
      projectID =
          config.BENCHMARK_WORK_MODE + "_" + config.DB_SWITCH + "_" + config.REMARK + "_" + labID;
      Date date = new Date(EXP_TIME);
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
      day = dateFormat.format(date);
      try {
        Class.forName(Constants.MYSQL_DRIVENAME);
        mysqlConnection = DriverManager.getConnection(config.MYSQL_URL);
        initTable();
      } catch (SQLException e) {
        LOGGER.error("mysql 初始化失败，原因是", e);
      } catch (ClassNotFoundException e) {
        LOGGER.error("mysql 连接初始化失败，原因是", e);
      }
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
              + "remark varchar(6000), primary key(id))");
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

  // 将插入测试的以batch为单位的中间结果存入数据库
  public void saveOperationResult(String operation, int okPoint, int failPoint,
      double latency, String remark) {
    if (config.IS_USE_MYSQL) {
      double rate = 0;
      if (latency > 0) {
        rate = okPoint * 1000 / latency; //unit: points/second
      }
      String time = df.format(new java.util.Date(System.currentTimeMillis()));
      String mysqlSql = String
          .format("insert into " + projectID + " values(NULL,'%s','%s','%s',%d,%d,%f,%f,'%s')",
              time, Thread.currentThread().getName(), operation, okPoint, failPoint, latency, rate,
              remark);
      try (Statement stat = mysqlConnection.createStatement()) {
        stat.executeUpdate(mysqlSql);
      } catch (Exception e) {
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
  }


  // 将系统资源利用信息存入mysql
  public void insertSERVER_MODE(double cpu, double mem, double io, double net_recv, double net_send,
      double pro_mem_size,
      double dataSize, double infoSize, double metadataSize, double overflowSize, double deltaSize,
      double walSize,
      float tps, float io_read, float io_wrtn,
      List<Integer> openFileList, String remark) {
    if (config.IS_USE_MYSQL) {
      Statement stat = null;
      String sql = "";
      try {
        stat = mysqlConnection.createStatement();
        sql = String.format("insert into SERVER_MODE_" + localName
                + "_" + day
                + " values(%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s)",
            System.currentTimeMillis(),
            cpu,
            mem,
            io,
            net_recv,
            net_send,
            pro_mem_size,
            dataSize,
            infoSize,
            metadataSize,
            overflowSize,
            deltaSize,
            walSize,
            tps,
            io_read,
            io_wrtn,
            openFileList.get(0),
            openFileList.get(1),
            openFileList.get(2),
            openFileList.get(3),
            openFileList.get(4),
            openFileList.get(5),
            openFileList.get(6),
            openFileList.get(7),
            openFileList.get(8),
            "'" + remark + "'");
        stat.executeUpdate(sql);
      } catch (SQLException e) {
        LOGGER.error("{}将SERVER_MODE写入mysql失败, because", sql, e);
      } finally {
        if (stat != null) {
          try {
            stat.close();
          } catch (SQLException e) {
            LOGGER.error("Failed to close statement for server monitoring because", e);
          }
        }
      }
    }
  }

  // 存储实验结果
  public void saveResult(String operation, String k, String v) {
    if (!config.IS_USE_MYSQL) {
      return;
    }
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

  public void saveTestConfig() {
    if (!config.IS_USE_MYSQL) {
      return;
    }
    Statement stat = null;
    String sql = "";
    try {
      stat = mysqlConnection.createStatement();
      if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH)) {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'MODE'", "'GEN_DATA_MODE'");
        stat.addBatch(sql);
      } else if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_QUERY_TEST_WITH_DEFAULT_PATH)) {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'MODE'", "'QUERY_TEST_MODE'");
        stat.addBatch(sql);
      } else {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'MODE'", "'INSERT_TEST_MODE'");
        stat.addBatch(sql);
      }
      switch (config.DB_SWITCH.trim()) {
        case Constants.DB_IOT:
        case Constants.DB_TIMESCALE:
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'ServerIP'", "'" + config.host + "'");
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
      if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH)) {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'STORAGE_GROUP_NAME'", "'" + config.STORAGE_GROUP_NAME + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'TIMESERIES_NAME'", "'" + config.TIMESERIES_NAME + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'TIMESERIES_TYPE'", "'" + config.TIMESERIES_TYPE + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'ENCODING'", "'" + config.ENCODING + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'BATCH_SIZE'", "'" + config.BATCH_SIZE + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'POINT_STEP'", "'" + config.POINT_STEP + "'");
        stat.addBatch(sql);
      } else if (config.BENCHMARK_WORK_MODE
          .equals(Constants.MODE_QUERY_TEST_WITH_DEFAULT_PATH)) {// 查询测试
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
            "'QUERY_CHOICE'",
            "'" + Constants.QUERY_CHOICE_NAME[config.QUERY_CHOICE]
                + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'QUERY_DEVICE_NUM'", "'" + config.QUERY_DEVICE_NUM
                + "'");
        stat.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'QUERY_SENSOR_NUM'", "'" + config.QUERY_SENSOR_NUM
                + "'");
        stat.addBatch(sql);
        switch (config.QUERY_CHOICE) {
          case 1:
            sql = String
                .format(SAVE_CONFIG, "'" + projectID + "'",
                    "'IS_RESULTSET_NULL'",
                    "'" + config.IS_EMPTY_PRECISE_POINT_QUERY
                        + "'");
            stat.addBatch(sql);
          case 3:
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                "'QUERY_AGGREGATE_FUN'", "'"
                    + config.QUERY_AGGREGATE_FUN + "'");
            stat.addBatch(sql);
            break;
          case 4:
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                "'TIME_INTERVAL'", "'" + config.QUERY_INTERVAL
                    + "'");
            stat.addBatch(sql);
            break;
          case 5:
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                "'FILTRATION_CONDITION'", "'values > "
                    + config.QUERY_LOWER_LIMIT + "'");
            stat.addBatch(sql);
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                "'TIME_INTERVAL'", "'" + config.QUERY_INTERVAL
                    + "'");
            stat.addBatch(sql);
            break;
          case 7:
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                "'FILTRATION_CONDITION'", "'values > "
                    + config.QUERY_LOWER_LIMIT + "'");
            stat.addBatch(sql);
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                "'TIME_INTERVAL'", "'" + config.QUERY_INTERVAL
                    + "'");
            stat.addBatch(sql);
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                "'TIME_UNIT'", "' " + config.TIME_UNIT + "'");
            stat.addBatch(sql);
            break;
        }
      } else {// 写入测试
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

  public void closeMysql() {
    if (config.IS_USE_MYSQL && mysqlConnection != null) {
      try {
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
