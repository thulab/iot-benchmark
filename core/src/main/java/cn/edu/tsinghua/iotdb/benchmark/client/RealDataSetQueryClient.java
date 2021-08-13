package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.workload.IRealDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.VerificationQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class RealDataSetQueryClient extends Client implements Runnable {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RealDataSetWriteClient.class);

  private final IRealDataWorkload realDataWorkload;
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  private long batchIndex = 0;

  public RealDataSetQueryClient(
      int id, CountDownLatch countDownLatch, CyclicBarrier barrier, IRealDataWorkload workload) {
    super(id, countDownLatch, barrier);
    realDataWorkload = workload;
  }

  @Override
  void doTest() {
    String currentThread = Thread.currentThread().getName();

    // print current progress periodically
    service.scheduleAtFixedRate(
        () -> {
          LOGGER.info(
              "{} {} % RealDataWorkload is done.", currentThread, batchIndex / config.getLOOP());
        },
        1,
        config.getLOG_PRINT_INTERVAL(),
        TimeUnit.SECONDS);

    while (true) {
      try {
        VerificationQuery verificationQuery = realDataWorkload.getVerifiedQuery();
        if (verificationQuery == null) {
          break;
        }
        dbWrapper.verificationQuery(verificationQuery);
        batchIndex++;
      } catch (Exception e) {
        LOGGER.error("Failed to insert one batch data because ", e);
      }
    }

    service.shutdown();
  }
}
