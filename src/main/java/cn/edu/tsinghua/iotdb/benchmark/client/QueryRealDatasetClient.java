package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.RealDatasetWorkLoad;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.slf4j.LoggerFactory;

public class QueryRealDatasetClient extends SyntheticBaseClient {

  public QueryRealDatasetClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier,
      Config config) {
    super(id, countDownLatch, barrier, new RealDatasetWorkLoad(config));
  }

  @Override
  void initLogger() {
    LOGGER = LoggerFactory.getLogger(QueryRealDatasetClient.class);
  }
}
