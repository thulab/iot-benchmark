package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.workload.SyntheticWorkload;
import java.util.concurrent.CountDownLatch;
import org.slf4j.LoggerFactory;

public class SyntheticClient extends SyntheticBaseClient {

  public SyntheticClient(int id, CountDownLatch countDownLatch) {
    super(id, countDownLatch, new SyntheticWorkload(id));
  }

  @Override
  void initLogger() {
    LOGGER = LoggerFactory.getLogger(SyntheticClient.class);
  }
}
