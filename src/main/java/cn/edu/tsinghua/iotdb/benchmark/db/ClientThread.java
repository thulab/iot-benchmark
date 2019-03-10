package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Storage;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientThread implements Runnable {

  private IDatebase database;
  private MySqlLog mySql;
  private int index;
  private Config config;
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientThread.class);
  private Storage storage;
  private static ThreadLocal<Long> totalTime = new ThreadLocal<Long>() {
    protected Long initialValue() {
      return (long) 0;
    }
  };
  private static ThreadLocal<Long> errorCount = new ThreadLocal<Long>() {
    protected Long initialValue() {
      return (long) 0;
    }
  };
  private CountDownLatch downLatch;
  private ArrayList<Long> totalTimes;
  private ArrayList<Long> totalInsertErrorNums;
  private ArrayList<Long> latencies;
  private ArrayList<ArrayList> latenciesOfClients;

  public ClientThread(IDatebase datebase, int index, CountDownLatch downLatch,
      ArrayList<Long> totalTimes, ArrayList<Long> totalInsertErrorNums,
      ArrayList<ArrayList> latenciesOfClients) {
    this.config = ConfigDescriptor.getInstance().getConfig();
    this.database = datebase;
    this.index = index;
    this.downLatch = downLatch;
    this.totalTimes = totalTimes;
    this.totalInsertErrorNums = totalInsertErrorNums;
    this.latencies = new ArrayList<>();
    this.latenciesOfClients = latenciesOfClients;
    mySql = new MySqlLog();
    mySql.initMysql(datebase.getLabID());
    System.out.println(Thread.currentThread().getId());
  }

  public ClientThread(IDatebase datebase, int index, Storage storage, CountDownLatch downLatch,
      ArrayList<Long> totalTimes, ArrayList<Long> totalInsertErrorNums,
      ArrayList<ArrayList> latenciesOfClients) {
    this.config = ConfigDescriptor.getInstance().getConfig();
    this.database = datebase;
    this.index = index;
    this.storage = storage;
    this.downLatch = downLatch;
    this.totalTimes = totalTimes;
    this.totalInsertErrorNums = totalInsertErrorNums;
    this.latencies = new ArrayList<>();
    this.latenciesOfClients = latenciesOfClients;
    mySql = new MySqlLog();
    mySql.initMysql(datebase.getLabID());
  }


  @Override
  public void run() {
    System.out.println(Thread.currentThread().getId());
    int i = 0;
    LinkedList<String> deviceCodes = new LinkedList<>();
    //may not correct in multiple device per batch mode
    long pointsOneLoop =
        config.DEVICE_NUMBER / config.CLIENT_NUMBER * config.SENSOR_NUMBER * config.CACHE_NUM;
    double actualLoopSecond = (double) pointsOneLoop / config.CLIENT_MAX_WRT_RATE;
    //overflow mode 2 related variables initial
    Random random = new Random(config.QUERY_SEED);
    ArrayList<Integer> before = new ArrayList<>();
    int maxIndex = (int) (config.CACHE_NUM * config.LOOP * config.OVERFLOW_RATIO);
    int currMaxIndexOfDist = config.START_TIMESTAMP_INDEX;
    deviceCodes.add("d_0");
    while (i < config.LOOP) {
      System.out.println("insert");
      long oldTotalTime = totalTime.get();
      if (!config.IS_OVERFLOW) {
        System.out.println("insert no overflow");
        try {
          database.insertOneBatch("d_0", i, totalTime, errorCount, latencies);
        } catch (SQLException e) {
          LOGGER.error("{} Fail to insert one batch into database becasue {}",
              Thread.currentThread().getName(), e.getMessage());
        }
      } else if (config.IS_OVERFLOW && config.OVERFLOW_MODE == 1) {
        System.out.println("insert overflow 1");
        try {
          maxIndex = database.insertOverflowOneBatch("d_0",
              i,
              totalTime,
              errorCount,
              before,
              maxIndex,
              random, latencies);
        } catch (SQLException e) {
          LOGGER.error("{} Fail to insert one batch into database becasue {}",
              Thread.currentThread().getName(), e.getMessage());
        }
      } else if (config.IS_OVERFLOW && config.OVERFLOW_MODE == 2) {
        System.out.println("insert overflow 2");
        try {
          currMaxIndexOfDist = database.insertOverflowOneBatchDist("d_0",
              i,
              totalTime,
              errorCount,
              currMaxIndexOfDist,
              random, latencies);
        } catch (SQLException e) {
          LOGGER.error("{} Fail to insert one batch into database becasue {}",
              Thread.currentThread().getName(), e.getMessage());
        }
      } else {
        System.out.println("unsupported overflow mode:" + config.OVERFLOW_MODE);
        break;
      }
      i++;
      long loopDeltaTime = totalTime.get() - oldTotalTime;

      double loopSecond = loopDeltaTime * 0.000000001d;

      double loopRate = pointsOneLoop / loopSecond;

      if (config.USE_OPS) {
        long delayStart = System.nanoTime();
        if (loopSecond < actualLoopSecond) {
          try {
            Thread.sleep((long) (1000 * (actualLoopSecond - loopSecond)));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        long delayEnd = System.nanoTime();
        long delayTime = delayEnd - delayStart;
        loopRate = pointsOneLoop / (loopSecond + delayTime * 0.000000001d);
      }
      LOGGER.info("LOOP RATE,{},points/s,LOOP DELTA TIME,{},second", loopRate, loopSecond);
      mySql.saveInsertProcessOfLoop(i, loopRate);
    }
    try {
      database.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    this.totalTimes.add(totalTime.get());
    this.totalInsertErrorNums.add(errorCount.get());
    this.latenciesOfClients.add(latencies);
    this.downLatch.countDown();
  }

}
