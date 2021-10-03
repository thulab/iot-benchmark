package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.distribution.PoissonDistribution;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SingletonWorkDataWorkLoad extends GenerateDataWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingletonWorkDataWorkLoad.class);
  private ConcurrentHashMap<Integer, AtomicLong> deviceMaxTimeIndexMap;
  private static SingletonWorkDataWorkLoad singletonWorkDataWorkLoad = null;
  private static AtomicInteger sensorIndex = new AtomicInteger();

  private SingletonWorkDataWorkLoad() {
    deviceMaxTimeIndexMap = new ConcurrentHashMap<>();
    for (int i = 0; i < config.getDEVICE_NUMBER(); i++) {
      deviceMaxTimeIndexMap.put(MetaUtil.getDeviceId(i), new AtomicLong(0));
    }
  }

  public static SingletonWorkDataWorkLoad getInstance() {
    if (singletonWorkDataWorkLoad == null) {
      synchronized (SingletonWorkDataWorkLoad.class) {
        if (singletonWorkDataWorkLoad == null) {
          singletonWorkDataWorkLoad = new SingletonWorkDataWorkLoad();
        }
      }
    }
    return singletonWorkDataWorkLoad;
  }

  @Override
  protected Batch getOrderedBatch() {
    long curLoop = insertLoop.getAndIncrement();
    DeviceSchema deviceSchema = getDeviceSchema(curLoop);
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      long stepOffset =
          (curLoop / config.getDEVICE_NUMBER()) * config.getBATCH_SIZE_PER_WRITE() + batchOffset;
      if (config.isIS_SENSOR_TS_ALIGNMENT()) {
        addOneRowIntoBatch(batch, stepOffset);
      } else {
        addOneRowIntoBatch(batch, stepOffset, sensorIndex.get());
      }
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  @Override
  protected Batch getDistOutOfOrderBatch() {
    long curLoop = insertLoop.getAndIncrement();
    DeviceSchema deviceSchema = getDeviceSchema(curLoop);
    int deviceId = deviceSchema.getDeviceId();

    Batch batch = new Batch();
    PoissonDistribution poissonDistribution = new PoissonDistribution(poissonRandom);
    int nextDelta;
    long stepOffset;
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      if (probTool.returnTrueByProb(config.getOUT_OF_ORDER_RATIO(), poissonRandom)) {
        // generate out of order timestamp
        nextDelta = poissonDistribution.getNextPoissonDelta();
        stepOffset = deviceMaxTimeIndexMap.get(deviceId).get() - nextDelta;
      } else {
        // generate normal increasing timestamp
        stepOffset = deviceMaxTimeIndexMap.get(deviceId).getAndIncrement();
      }
      if (config.isIS_SENSOR_TS_ALIGNMENT()) {
        addOneRowIntoBatch(batch, stepOffset);
      } else {
        addOneRowIntoBatch(batch, stepOffset, sensorIndex.get());
      }
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  private DeviceSchema getDeviceSchema(long loop) {
    List<String> sensors = new ArrayList<>();
    if (config.isIS_SENSOR_TS_ALIGNMENT()) {
      sensors = config.getSENSOR_CODES();
    } else {
      int sensorId = sensorIndex.getAndIncrement();
      sensors.add(config.getSENSOR_CODES().get(sensorId));
      if (sensorIndex.get() >= config.getSENSOR_NUMBER()) {
        sensorIndex.set(0);
      }
    }
    DeviceSchema deviceSchema =
        new DeviceSchema(MetaUtil.getDeviceId((int) loop % config.getDEVICE_NUMBER()), sensors);
    return deviceSchema;
  }

  @Override
  protected Batch getLocalOutOfOrderBatch() {
    LOGGER.error("Not supported OUT_OF_ORDER_MODE = 1 when IS_CLIENT_BIND = false");
    return null;
  }
}
