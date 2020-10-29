package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.mysql;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
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
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlRecorder implements ITestDataPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySqlRecorder.class);
  private static final String SAVE_CONFIG = "insert into CONFIG values(NULL, %s, %s, %s)";
  private static final String SAVE_RESULT = "insert into FINAL_RESULT values(NULL, '%s', '%s', '%s', '%s')";
  private Connection mysqlConnection = null;
  private final Config config = ConfigDescriptor.getInstance().getConfig();
  private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");
  private String localName;
  private final String day;
  private static final long EXP_TIME = System.currentTimeMillis();
  private final String projectID = String.format("%s_%s_%s_%s",config.getBENCHMARK_WORK_MODE(), config.getDB_SWITCH(), config.getREMARK(), sdf.format(new java.util.Date(EXP_TIME)));
  private Statement statement;
  private static final String URL_TEMPLATE = "jdbc:mysql://%s:%s/%s?user=%s&password=%s&useUnicode=true&characterEncoding=UTF8&useSSL=false&rewriteBatchedStatements=true";
  private final String url = String.format(URL_TEMPLATE, config.getTEST_DATA_STORE_IP(),
      config.getTEST_DATA_STORE_PORT(), config.getTEST_DATA_STORE_DB(), config.getTEST_DATA_STORE_USER(), config.getTEST_DATA_STORE_PW());
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
      mysqlConnection = DriverManager.getConnection(url);
      statement = mysqlConnection.createStatement();
      initTable();
    } catch (SQLException e) {
      LOGGER.error("mysql 初始化失败，原因是", e);
    } catch (ClassNotFoundException e) {
      LOGGER.error("mysql 连接初始化失败，原因是", e);
    }

  }

  // 检查记录本次实验的表格是否已经创建，没有则创建
  private void initTable() {
    try {
      if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_SERVER_MODE)
          || config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_CLIENT_SYSTEM_INFO)) {
        if (!hasTable("SERVER_MODE_" + localName + "_" + day)) {
          statement.executeUpdate("create table SERVER_MODE_"
              + localName
              + "_"
              + day
              + "(id BIGINT, "
              + "cpu_usage DOUBLE,mem_usage DOUBLE,diskIo_usage DOUBLE,net_recv_rate DOUBLE,net_send_rate DOUBLE, pro_mem_size DOUBLE, "
              + "dataFileSize DOUBLE,systemFizeSize DOUBLE,sequenceFileSize DOUBLE,unsequenceFileSize DOUBLE, walFileSize DOUBLE,"
              + "tps DOUBLE,MB_read DOUBLE,MB_wrtn DOUBLE,"
              + "primary key(id))");
          LOGGER.info("Table SERVER_MODE create success!");
        }
        return;
      }
      if (!hasTable("CONFIG")) {
        statement.executeUpdate(
            "create table CONFIG (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, projectID VARCHAR(150), configuration_item VARCHAR(150), configuration_value VARCHAR(150))AUTO_INCREMENT = 1;");
        LOGGER.info("Table CONFIG create success!");
      }
      if (!hasTable("FINAL_RESULT")) {
        statement.executeUpdate(
            "create table FINAL_RESULT (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, projectID VARCHAR(150), operation VARCHAR(50), result_key VARCHAR(150), result_value VARCHAR(150))AUTO_INCREMENT = 1;");
        LOGGER.info("Table FINAL_RESULT create success!");
      }
      if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH) && !hasTable(
          projectID)) {
        statement.executeUpdate("create table "
            + projectID
            + "(id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, recordTime varchar(50), clientName varchar(50), "
            + "operation varchar(50), okPoint INTEGER, failPoint INTEGER, latency DOUBLE, rate DOUBLE, remark varchar(1000))AUTO_INCREMENT = 1;");
        LOGGER.info("Table {} create success!", projectID);
      }
    } catch (SQLException e) {
      LOGGER.error("mysql 创建表格失败,原因是", e);
    }
  }

  @Override
  public void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap) {
    String sql = "";
    try {
      sql = String.format("insert into SERVER_MODE_" + localName
              + "_" + day + " values(%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f)",
          System.currentTimeMillis(),
          systemMetricsMap.get(SystemMetrics.CPU_USAGE),
          systemMetricsMap.get(SystemMetrics.MEM_USAGE),
          systemMetricsMap.get(SystemMetrics.DISK_IO_USAGE),
          systemMetricsMap.get(SystemMetrics.NETWORK_R_RATE),
          systemMetricsMap.get(SystemMetrics.NETWORK_S_RATE),
          systemMetricsMap.get(SystemMetrics.PROCESS_MEM_SIZE),
          systemMetricsMap.get(SystemMetrics.DATA_FILE_SIZE),
          systemMetricsMap.get(SystemMetrics.SYSTEM_FILE_SIZE),
          systemMetricsMap.get(SystemMetrics.SEQUENCE_FILE_SIZE),
          systemMetricsMap.get(SystemMetrics.UN_SEQUENCE_FILE_SIZE),
          systemMetricsMap.get(SystemMetrics.WAL_FILE_SIZE),
          systemMetricsMap.get(SystemMetrics.DISK_TPS),
          systemMetricsMap.get(SystemMetrics.DISK_READ_SPEED_MB),
          systemMetricsMap.get(SystemMetrics.DISK_WRITE_SPEED_MB));
      statement.executeUpdate(sql);
    } catch (SQLException e) {
      LOGGER.error("{} insert into mysql failed", sql, e);
    }
  }

  // 将插入测试的以batch为单位的中间结果存入数据库
  @Override
  public void saveOperationResult(String operation, int okPoint, int failPoint,
      double latency, String remark) {
    if(config.getCURRENT_CSV_LINE() % 10 < config.getMYSQL_REAL_INSERT_RATE() * 10) {
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
        statement.execute(mysqlSql);
        count++;
        if (count % 5000 == 0) {
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
              mysqlConnection = DriverManager.getConnection(url);
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

  // 存储实验结果
  @Override
  public void saveResult(String operation, String k, String v) {
    String sql = String.format(SAVE_RESULT, projectID, operation, k, v);
    try {
      statement.executeUpdate(sql);
    } catch (SQLException e) {
      LOGGER.error("{}将结果信息写入mysql失败，because ：{}", sql, e);
    }
  }

  @Override
  public void saveTestConfig() {
    String sql = "";
    try {
      if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'MODE'", "'DEFAULT_TEST_MODE'");
        statement.addBatch(sql);
      }
      switch (config.getDB_SWITCH().trim()) {
        case Constants.DB_IOT:
        case Constants.DB_TIMESCALE:
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'ServerIP'", "'" + config.getHOST() + "'");
          statement.addBatch(sql);
          break;
        case Constants.DB_INFLUX:
        case Constants.DB_OPENTS:
        case Constants.DB_KAIROS:
        case Constants.DB_CTS:
          String host = config.getDB_URL()
              .substring(config.getDB_URL().lastIndexOf('/') + 1, config.getDB_URL().lastIndexOf(':'));
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'ServerIP'", "'" + host + "'");
          statement.addBatch(sql);
          break;
        default:
          throw new SQLException("unsupported database " + config.getDB_SWITCH());
      }
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
          "'CLIENT'", "'" + localName + "'");
      statement.addBatch(sql);
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
          "'DB_SWITCH'", "'" + config.getDB_SWITCH() + "'");
      statement.addBatch(sql);
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
          "'VERSION'", "'" + config.getVERSION() + "'");
      statement.addBatch(sql);
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
          "'getCLIENT_NUMBER()'", "'" + config.getCLIENT_NUMBER() + "'");
      statement.addBatch(sql);
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'", "'LOOP'",
          "'" + config.getLOOP() + "'");
      statement.addBatch(sql);
      if (config.getBENCHMARK_WORK_MODE()
          .equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'查询数据集存储组数'",
            "'" + config.getGROUP_NUMBER() + "'");
        statement.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'查询数据集设备数'", "'" + config.getDEVICE_NUMBER()
                + "'");
        statement.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'查询数据集传感器数'", "'" + config.getSENSOR_NUMBER()
                + "'");
        statement.addBatch(sql);
        if (config.getDB_SWITCH().equals(Constants.DB_IOT)) {
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'IOTDB编码方式'", "'" + config.getENCODING() + "'");
          statement.addBatch(sql);
        }

        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'QUERY_DEVICE_NUM'", "'" + config.getQUERY_DEVICE_NUM()
                + "'");
        statement.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'QUERY_SENSOR_NUM'", "'" + config.getQUERY_SENSOR_NUM()
                + "'");
        statement.addBatch(sql);

        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'IS_OVERFLOW'", "'" + config.isIS_OVERFLOW() + "'");
        statement.addBatch(sql);
        if (config.isIS_OVERFLOW()) {
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'OVERFLOW_RATIO'", "'" + config.getOVERFLOW_RATIO() + "'");
          statement.addBatch(sql);
        }
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'MUL_DEV_BATCH'", "'" + config.isMUL_DEV_BATCH() + "'");
        statement.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'DEVICE_NUMBER'", "'" + config.getDEVICE_NUMBER() + "'");
        statement.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'GROUP_NUMBER'", "'" + config.getGROUP_NUMBER() + "'");
        statement.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'DEVICE_NUMBER'", "'" + config.getDEVICE_NUMBER() + "'");
        statement.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'SENSOR_NUMBER'", "'" + config.getSENSOR_NUMBER() + "'");
        statement.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'BATCH_SIZE'", "'" + config.getBATCH_SIZE() + "'");
        statement.addBatch(sql);
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
            "'POINT_STEP'", "'" + config.getPOINT_STEP() + "'");
        statement.addBatch(sql);
        if (config.getDB_SWITCH().equals(Constants.DB_IOT)) {
          sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
              "'ENCODING'", "'" + config.getENCODING() + "'");
          statement.addBatch(sql);
        }
      }
      statement.executeBatch();
    } catch (SQLException e) {
      LOGGER.error("{}将配置信息写入mysql失败，because ：{}", sql, e);
    }
  }

  @Override
  public void close() {
    if (mysqlConnection != null) {
      try {
        statement.executeBatch();
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
