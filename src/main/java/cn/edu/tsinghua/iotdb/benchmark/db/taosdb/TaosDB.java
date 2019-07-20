package cn.edu.tsinghua.iotdb.benchmark.db.taosdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.QueryClientThread;
import cn.edu.tsinghua.iotdb.benchmark.db.iotdb.IoTDB;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Point;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaosDB implements IDatebase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private static final String TAOS_DRIVER = "com.taosdata.jdbc.TSDBDriver";
  private static final String URL_TAOS = "jdbc:TAOS://%s:%s/?user=%s&password=%s";
  private static final String USER = "root";
  private static final String PASSWD = "taosdata";
  private static final String CREATE_DATABASE = "create database if not exists %s";
  private static final String TEST_DB = "ZC";
  private static final String USE_DB = "use %s";
  private static final String CREATE_STABLE = "create table if not exists %s (ts timestamp, value float) tags(device binary(20),sensor binary(20))";
  private static final String CREATE_TABLE = "create table if not exists %s%s using %s tags('%s','%s')";
  private static final String INSERT_STAT = "%s%s values(%s,%s) ";
  private Connection connection;
  private static Config config;
  private List<Point> points;
  private Map<String, String> mp;
  private long labID;
  private MySqlLog mySql;
  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private Random sensorRandom;
  private Random timestampRandom;
  private ProbTool probTool;
  private final double unitTransfer = 1000000000.0;

  public TaosDB(long labID) throws ClassNotFoundException, SQLException {
    Class.forName(TAOS_DRIVER);
    config = ConfigDescriptor.getInstance().getConfig();
    points = new ArrayList<>();
    mp = new HashMap<>();
    mySql = new MySqlLog();
    this.labID = labID;
    sensorRandom = new Random(1 + config.QUERY_SEED);
    timestampRandom = new Random(2 + config.QUERY_SEED);
    probTool = new ProbTool();
    connection = DriverManager
        .getConnection(String.format(URL_TAOS, config.host, config.port, USER, PASSWD));
    mySql.initMysql(labID);
  }

  @Override
  public void init() throws SQLException {
    //delete old data of IoTDB is done in script cli-benchmark.sh
  }

  @Override
  public void createSchema() throws SQLException {
    Statement statement = connection.createStatement();
    statement.execute(String.format(CREATE_DATABASE, TEST_DB));
    statement.execute(String.format(USE_DB, TEST_DB));
    for (int i = 0; i < config.DEVICE_NUMBER; i++) {
      statement.execute(String.format(CREATE_STABLE, config.DEVICE_CODES.get(i)));
      for (String sensor : config.SENSOR_CODES) {
        statement.execute(String
            .format(CREATE_TABLE, config.DEVICE_CODES.get(i), sensor, config.DEVICE_CODES.get(i),
                config.DEVICE_CODES.get(i), sensor));
      }
    }
  }

  @Override
  public long getLabID() {
    return this.labID;
  }

  @Override
  public void insertOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
    Statement statement;
    long errorNum = 0;
    try {
      statement = connection.createStatement();
      if (!config.IS_OVERFLOW) {
        for (int i = 0; i < config.BATCH_SIZE; i++) {
          String sql = createSQLStatment(loopIndex, i, device);
          LOGGER.info("添加语句{}", sql);
          statement.addBatch(sql);
        }
      } else {
        LOGGER.error("TAOS数据库不支持乱序插入");
        throw new RuntimeException();
      }
      long startTime = System.nanoTime();
      try {
        statement.executeBatch();
      } catch (BatchUpdateException e) {
        long[] arr = e.getLargeUpdateCounts();
        for (long i : arr) {
          if (i == -3) {
            errorNum++;
          }
        }
      }
      statement.clearBatch();
      statement.close();
      long endTime = System.nanoTime();
      long costTime = endTime - startTime;
      latencies.add(costTime);
      if (errorNum > 0) {
        LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);
      } else {
//                LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
//                        Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
//                        (totalTime.get() + costTime) / unitTransfer,
//                        (config.BATCH_SIZE * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
        totalTime.set(totalTime.get() + costTime);
      }
      errorCount.set(errorCount.get() + errorNum);

      mySql.saveInsertProcess(loopIndex, (endTime - startTime) / unitTransfer,
          totalTime.get() / unitTransfer, errorNum,
          config.REMARK);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<
      Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {

  }

  @Override
  public void close() throws SQLException {
    if (connection != null) {
      connection.close();
    }
    if (mySql != null) {
      mySql.closeMysql();
    }
  }

  @Override
  public long getTotalTimeInterval() throws SQLException {
    return 0;
  }

  @Override
  public void executeOneQuery(List<Integer> devices, int index, long startTime,
      QueryClientThread client, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) {

  }

  @Override
  public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex,
      ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies)
      throws SQLException {

  }

  @Override
  public long count(String group, String device, String sensor) {
    return 0;
  }

  @Override
  public void createSchemaOfDataGen() throws SQLException {

  }

  @Override
  public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {

  }

  @Override
  public void exeSQLFromFileByOneBatch() throws SQLException, IOException {

  }

  @Override
  public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex,
      Random random, ArrayList<Long> latencies) throws SQLException {
    return 0;
  }

  @Override
  public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<
      Long> totalTime,
      ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random,
      ArrayList<Long> latencies) throws SQLException {
    return 0;
  }

  public String createSQLStatment(int batch, int index, String device) {
    StringBuilder builder = new StringBuilder();
    builder.append("insert into ");
    long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batch * config.BATCH_SIZE
        + index);
    if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
      currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
    }
    for (String sensor : config.SENSOR_CODES) {
      FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
      Number value = Function.getValueByFuntionidAndParam(param, currentTime);
      float v = Float.parseFloat(String.format("%.2f", value.floatValue()));
      builder.append(String.format(INSERT_STAT, device, sensor, currentTime, v));
    }
    LOGGER.debug("createSQLStatment:  {}", builder.toString());
    return builder.toString();
  }
}
