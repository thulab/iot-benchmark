package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.workload.IRealDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public abstract class RealDataClient extends Client implements Runnable {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RealDataClient.class);

  private final OperationController operationController;
  private final IRealDataWorkload realDataWorkload;
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  private long batchIndex = 0;

  public RealDataClient(
      int id, CountDownLatch countDownLatch, CyclicBarrier barrier, IRealDataWorkload workload) {
    super(id, countDownLatch, barrier);
    realDataWorkload = workload;
    operationController = new OperationController(id);
  }

  @Override
  void doTest() {
    String currentThread = Thread.currentThread().getName();

    // print current progress periodically
    service.scheduleAtFixedRate(
        () -> {
          LOGGER.info("{} {} batch RealDataWorkload is done.", currentThread, batchIndex);
        },
        1,
        config.getLOG_PRINT_INTERVAL(),
        TimeUnit.SECONDS);

    while (true) {
      try {
        Batch batch = realDataWorkload.getOneBatch();
        if (batch == null) {
          break;
        }
        dbWrapper.insertOneBatch(batch);
        batchIndex++;
      } catch (DBConnectException e) {
        LOGGER.error("Failed to insert one batch data because ", e);
      } catch (Exception e) {
        LOGGER.error("Failed to insert one batch data because ", e);
      }
    }

    service.shutdown();
  }
}
