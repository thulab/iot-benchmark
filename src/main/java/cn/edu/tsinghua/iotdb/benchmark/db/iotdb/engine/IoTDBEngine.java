package cn.edu.tsinghua.iotdb.benchmark.db.iotdb.engine;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.QueryClientThread;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Point;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iotdb.db.api.ITSEngine;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBEngine implements IDatebase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBEngine.class);
  private static Config config;
  private List<Point> points;
  private Map<String, String> mp;
  private IoTDBSingletonHelper db;
  private long labID;
  private MySqlLog mySql;
  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private Random sensorRandom;
  private Random timestampRandom;
  private ProbTool probTool;
  private final double unitTransfer = 1000000000.0;

  public IoTDBEngine(long libID) {
    config = ConfigDescriptor.getInstance().getConfig();
    points = new ArrayList<>();
    mp = new HashMap<>();
    mySql = new MySqlLog();
    this.labID = labID;
    sensorRandom = new Random(1 + config.QUERY_SEED);
    timestampRandom = new Random(2 + config.QUERY_SEED);
    probTool = new ProbTool();
    db = IoTDBSingletonHelper.getInstance();
    mySql.initMysql(labID);
  }

  @Override
  public void init() throws SQLException {
    // not need to implement
  }

  @Override
  public void createSchema() throws SQLException {
    // 根据条件在主线程中创建对应的timeseries和storage group
    int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
    ArrayList<String> group = new ArrayList<>();
    for (int i = 0; i < config.GROUP_NUMBER; i++) {
      group.add(Constants.ROOT_SERIES_NAME + "." + "group_" + i);
    }
    for (String g : group) {
      setStorgeGroup(g);
    }
    int count = 0;
    int groupIndex = 0;
    int timeseriesCount = 0;
    String path;
    for (String device : config.DEVICE_CODES) {
      if (count == groupSize) {
        groupIndex++;
        count = 0;
      }
      path = group.get(groupIndex) + "." + device;
      for (String sensor : config.SENSOR_CODES) {
        timeseriesCount++;
        createTimeseriesBatch(path, sensor, timeseriesCount);
      }
      count++;
    }
  }

  private void setStorgeGroup(String g) {
    ITSEngine engine = db.getEngine();
    try {
      System.out.println("storage group path: " + g);
      engine.setStorageGroup(g);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void createTimeseriesBatch(String path, String sensor, int count) {
    ITSEngine engine = db.getEngine();
    String timeseries = path + "." + sensor;
    try {
      System.out.println("time series: " + timeseries);
      engine.addTimeSeries(timeseries, config.DATA_TYPE.toString(), config.ENCODING.toString(),
          new String[0]);
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (count % 1000 == 0) {
      LOGGER.info("execute 1000 time series.");
    }
  }

  @Override
  public long getLabID() {
    return this.labID;
  }

  @Override
  public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
    long errorNum = 0;
    ITSEngine engine = db.getEngine();
    long startTime = System.nanoTime();
    if (!config.IS_OVERFLOW) {
      for (int i = 0; i < config.CACHE_NUM; i++) {
        errorNum += insertOneRow(engine, batchIndex, i, device);
      }
    } else {
      //随机重排，无法准确控制overflow比例
      int shuffleSize = (int) (config.OVERFLOW_RATIO * config.CACHE_NUM);
      int[] shuffleSequence = new int[shuffleSize];
      for (int i = 0; i < shuffleSize; i++) {
        shuffleSequence[i] = i;
      }

      int tmp = shuffleSequence[shuffleSize - 1];
      shuffleSequence[shuffleSize - 1] = shuffleSequence[0];
      shuffleSequence[0] = tmp;

      for (int i = 0; i < shuffleSize; i++) {
        errorNum += insertOneRow(engine, batchIndex, i, device);
      }
      for (int i = shuffleSize; i < config.CACHE_NUM; i++) {
        errorNum += insertOneRow(engine, batchIndex, i, device);
      }
    }

    long endTime = System.nanoTime();
    long costTime = endTime - startTime;
    latencies.add(costTime);
    if (errorNum > 0) {
      LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);
    } else {
      LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
          Thread.currentThread().getName(), batchIndex, costTime / unitTransfer,
          (totalTime.get() + costTime) / unitTransfer,
          (config.CACHE_NUM * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
      totalTime.set(totalTime.get() + costTime);
    }
    errorCount.set(errorCount.get() + errorNum);

    mySql.saveInsertProcess(batchIndex, (endTime - startTime) / unitTransfer,
        totalTime.get() / unitTransfer, errorNum,
        config.REMARK);
  }

  public int insertOneRow(ITSEngine engine, int batch, int index, String device) {
    String path = getGroupDevicePath(device);
    device = Constants.ROOT_SERIES_NAME + "." + path;
    long currentTime =
        Constants.START_TIMESTAMP + config.POINT_STEP * (batch * config.CACHE_NUM + index);
    List<String> sensors = config.SENSOR_CODES;

    List<String> values = new ArrayList<>();
    for (String sensor : config.SENSOR_CODES) {
      FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
      values.add(Function.getValueByFuntionidAndParam(param, currentTime).toString());
    }
    try {
      engine.write(device, currentTime, sensors, values);
    } catch (IOException e) {
      e.printStackTrace();
      return 1;
    }
    return 0;
  }

  private String getGroupDevicePath(String device) {
    String[] spl = device.split("_");
    int deviceIndex = Integer.parseInt(spl[1]);
    int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
    int groupIndex = deviceIndex / groupSize;
    return "group_" + groupIndex + "." + device;
  }

  @Override
  public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<
      Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
    throw new RuntimeException("not support");
  }

  @Override
  public void close() throws SQLException {
    // this.db.closeInstance();
  }

  @Override
  public long getTotalTimeInterval() throws SQLException {
    throw new RuntimeException("not support");
  }

  private String getFullGroupDevicePathByID(int id) {
    int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
    int groupIndex = id / groupSize;
    return Constants.ROOT_SERIES_NAME + ".group_" + groupIndex + "." + config.DEVICE_CODES.get(id);
  }

  @Override
  public void executeOneQuery(List<Integer> devices, int index, long startTime,
      QueryClientThread client, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) {
    long startTimeStamp = 0, endTimeStamp = 0, latency = 0;
    startTimeStamp = System.nanoTime();
    ITSEngine engine = db.getEngine();
    List<String> sensorList = new ArrayList<String>();
    String timesries = null;
    QueryDataSet queryDataSet = null;
    switch (config.QUERY_CHOICE) {
      case 4:// 范围查询
        List<String> list = new ArrayList<String>();
        for (String sensor : config.SENSOR_CODES) {
          list.add(sensor);
        }
        Collections.shuffle(list, sensorRandom);
        timesries = getFullGroupDevicePathByID(devices.get(0)).toString();
        timesries = timesries + "." + list.get(0);
        try {
          queryDataSet = engine.query(timesries, startTime, startTime + config.QUERY_INTERVAL);
        } catch (IOException e) {
          e.printStackTrace();
        }
        break;
      default:
        throw new RuntimeException("not support");

    }
    int line = 0;
    LOGGER.info("{} execute {} loop,提交执行的sql：{}, startTime: {}, endTime: {}.",
        Thread.currentThread().getName(), index, timesries, startTime,
        startTime + config.QUERY_INTERVAL);
    try {
      while (queryDataSet.hasNext()) {
        queryDataSet.next();
        line++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    endTimeStamp = System.nanoTime();
    latency = endTimeStamp - startTimeStamp;
    latencies.add(latency);
    client.setTotalPoint(
        client.getTotalPoint() + line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM);
    client.setTotalTime(client.getTotalTime() + latency);

    LOGGER.info(
        "{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
            + "TotalTime {}s with totalPoint {} rate is {}points/s",
        Thread.currentThread().getName(), index, (latency / 1000.0) / 1000000.0,
        line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM,
        line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM * 1000.0 / (latency / 1000000.0),
        (client.getTotalTime() / 1000.0) / 1000000.0, client.getTotalPoint(),
        client.getTotalPoint() * 1000.0f / (client.getTotalTime() / 1000000.0));
    mySql.saveQueryProcess(index, line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM,
        (latency / 1000.0f) / 1000000.0, config.REMARK);
  }

  @Override
  public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex,
      ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies)
      throws SQLException {
    throw new RuntimeException("not support");
  }

  @Override
  public long count(String group, String device, String sensor) {
    throw new RuntimeException("not support");
  }

  @Override
  public void createSchemaOfDataGen() throws SQLException {
    throw new RuntimeException("not support");
  }

  @Override
  public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
    throw new RuntimeException("not support");
  }

  @Override
  public void exeSQLFromFileByOneBatch() throws SQLException, IOException {
    throw new RuntimeException("not support");
  }

  @Override
  public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex,
      Random random, ArrayList<Long> latencies) throws SQLException {
    throw new RuntimeException("not support");
  }

  @Override
  public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<
      Long> totalTime,
      ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random,
      ArrayList<Long> latencies) throws SQLException {
    throw new RuntimeException("not support");
  }
}
