package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.RealDatasetWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class RealDatasetClient extends Client implements Runnable {

  private RealDatasetWorkLoad workload;

  public RealDatasetClient(int id, CountDownLatch countDownLatch, Config config,
      List<String> files) {
    super(id, countDownLatch);
    workload = new RealDatasetWorkLoad(files, config);
  }

  void doTest() {
    try {
      while (true) {
        Batch batch = workload.getOneBatch();
        if (batch == null) {
          break;
        }
        dbWrapper.insertOneBatch(batch);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
