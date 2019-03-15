package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.DBWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private DBWrapper dbWrapper;

  Client(){

  }

  @Override
  public void run() {
    switch (config.BENCHMARK_WORK_MODE) {
      case Constants.MODE_TEST_WITH_DEFAULT_PATH:
        doTestWithDefaultPath();
        break;
      default:

    }
  }

  private void doTestWithDefaultPath() {
    int loopIndex = 0;


  }
}
