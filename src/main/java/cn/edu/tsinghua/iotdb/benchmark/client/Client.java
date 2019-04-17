package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Client implements Runnable{

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
  protected static Config config = ConfigDescriptor.getInstance().getConfig();
  protected Measurement measurement;
  private CountDownLatch countDownLatch;
  private CountDownLatch startLatch;
  int clientThreadId;
  DBWrapper dbWrapper;

  public Client(int id, CountDownLatch countDownLatch, CountDownLatch startLatch) {
    this.countDownLatch = countDownLatch;
    this.startLatch = startLatch;
    clientThreadId = id;
    measurement = new Measurement();
    dbWrapper = new DBWrapper(measurement);
  }

  public Measurement getMeasurement() {
    return measurement;
  }

  @Override
  public void run() {
    try {
      try {
        dbWrapper.init();
        startLatch.await(); // wait for that all clients start test simultaneously
        doTest();
      } catch (Exception e) {
        LOGGER.error("Unexpected error: ", e);
      } finally {
        try {
          dbWrapper.close();
        } catch (TsdbException e) {
          LOGGER.error("Close {} error: ", config.DB_SWITCH, e);
        }
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  abstract void doTest();

}
