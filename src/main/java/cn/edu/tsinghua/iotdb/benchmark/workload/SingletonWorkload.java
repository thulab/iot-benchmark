package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingletonWorkload {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingletonWorkload.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private ProbTool probTool;
  private Random poissonRandom;
  private AtomicLong insertLoop;
  private ConcurrentHashMap<Integer, AtomicLong> deviceMaxTimeIndexMap;


  private static class SingletonWorkloadHolder {

    private static final SingletonWorkload INSTANCE = new SingletonWorkload();
  }

  public static SingletonWorkload getInstance() {
    return SingletonWorkloadHolder.INSTANCE;
  }

  private SingletonWorkload() {
    insertLoop = new AtomicLong(0);
    deviceMaxTimeIndexMap = new ConcurrentHashMap<>();
    for (int i = 0; i < config.DEVICE_NUMBER; i++) {
      deviceMaxTimeIndexMap.put(i, new AtomicLong(0));
    }
    probTool = new ProbTool();
    poissonRandom = new Random(config.DATA_SEED);
  }

  private Batch getOrderedBatch() {
    long curLoop = insertLoop.getAndIncrement();
    DeviceSchema deviceSchema = new DeviceSchema((int) (curLoop % config.DEVICE_NUMBER));
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
      long stepOffset = (curLoop / config.DEVICE_NUMBER) * config.BATCH_SIZE + batchOffset;
      Workload.addOneRowIntoBatch(deviceSchema, batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  private Batch getDistOutOfOrderBatch() {
    long curLoop = insertLoop.getAndIncrement();
    int deviceIndex = (int) (curLoop % config.DEVICE_NUMBER);
    DeviceSchema deviceSchema = new DeviceSchema(deviceIndex);
    Batch batch = new Batch();
    PossionDistribution possionDistribution = new PossionDistribution(poissonRandom);
    int nextDelta;
    long stepOffset;
    for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
      if (probTool.returnTrueByProb(config.OVERFLOW_RATIO, poissonRandom)) {
        // generate overflow timestamp
        nextDelta = possionDistribution.getNextPossionDelta();
        stepOffset = deviceMaxTimeIndexMap.get(deviceIndex).get() - nextDelta;
      } else {
        // generate normal increasing timestamp
        stepOffset = deviceMaxTimeIndexMap.get(deviceIndex).getAndIncrement();
      }
      Workload.addOneRowIntoBatch(deviceSchema, batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  private Batch getLocalOutOfOrderBatch() {
    return null;
  }

  private Batch getGlobalOutOfOrderBatch() {
    return null;
  }

  public Batch getOneBatch() throws WorkloadException {
    if (!config.IS_OVERFLOW) {
      return getOrderedBatch();
    } else {
      switch (config.OVERFLOW_MODE) {
        case 0:
          return getLocalOutOfOrderBatch();
        case 1:
          return getGlobalOutOfOrderBatch();
        case 2:
          return getDistOutOfOrderBatch();
        default:
          throw new WorkloadException("Unsupported overflow mode: " + config.OVERFLOW_MODE);
      }
    }
  }

}
