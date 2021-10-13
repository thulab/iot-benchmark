package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.distribution.PoissonDistribution;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;

import java.util.*;

public class SyntheticDataWorkLoad extends GenerateDataWorkLoad {

  private final Map<DeviceSchema, Long> maxTimestampIndexMap;
  private long insertLoop = 0;
  private int deviceIndex = 0;
  private int sensorIndex = 0;

  public SyntheticDataWorkLoad(List<DeviceSchema> deviceSchemas) {
    if (config.isIS_OUT_OF_ORDER()) {
      long startIndex = (long) (config.getLOOP() * config.getOUT_OF_ORDER_RATIO());
      insertLoop = startIndex;
    }
    this.deviceSchemas = deviceSchemas;
    maxTimestampIndexMap = new HashMap<>();
    for (DeviceSchema schema : deviceSchemas) {
      if (config.isIS_SENSOR_TS_ALIGNMENT()) {
        maxTimestampIndexMap.put(schema, 0L);
      } else {
        for (Sensor sensor : schema.getSensors()) {
          DeviceSchema deviceSchema = new DeviceSchema(schema.getDeviceId(), Arrays.asList(sensor));
          maxTimestampIndexMap.put(deviceSchema, 0L);
        }
      }
    }
    this.deviceSchemaSize = deviceSchemas.size();
  }

  @Override
  protected Batch getOrderedBatch() {
    DeviceSchema deviceSchema = getDeviceSchema();
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      long stepOffset = insertLoop * config.getBATCH_SIZE_PER_WRITE() + batchOffset;
      if (config.isIS_SENSOR_TS_ALIGNMENT()) {
        addOneRowIntoBatch(batch, stepOffset);
      } else {
        addOneRowIntoBatch(batch, stepOffset, sensorIndex);
      }
    }
    batch.setDeviceSchema(deviceSchema);
    next();
    return batch;
  }

  private DeviceSchema getDeviceSchema() {
    DeviceSchema deviceSchema =
        new DeviceSchema(
            deviceSchemas.get(deviceIndex).getDeviceId(),
            deviceSchemas.get(deviceIndex).getSensors());
    if (!config.isIS_SENSOR_TS_ALIGNMENT()) {
      List<Sensor> sensors = new ArrayList<>();
      sensors.add(deviceSchema.getSensors().get(sensorIndex));
      deviceSchema.setSensors(sensors);
    }
    return deviceSchema;
  }

  @Override
  protected Batch getDistOutOfOrderBatch() {
    DeviceSchema deviceSchema = getDeviceSchema();
    Batch batch = new Batch();
    PoissonDistribution poissonDistribution = new PoissonDistribution(poissonRandom);
    int nextDelta;
    long stepOffset;
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      if (probTool.returnTrueByProb(config.getOUT_OF_ORDER_RATIO(), poissonRandom)) {
        // generate out of order timestamp
        nextDelta = poissonDistribution.getNextPoissonDelta();
        stepOffset = maxTimestampIndexMap.get(deviceSchema) - nextDelta;
      } else {
        // generate normal increasing timestamp
        maxTimestampIndexMap.put(deviceSchema, maxTimestampIndexMap.get(deviceSchema) + 1);
        stepOffset = maxTimestampIndexMap.get(deviceSchema);
      }
      if (config.isIS_SENSOR_TS_ALIGNMENT()) {
        addOneRowIntoBatch(batch, stepOffset);
      } else {
        addOneRowIntoBatch(batch, stepOffset, sensorIndex);
      }
    }
    batch.setDeviceSchema(deviceSchema);
    next();
    return batch;
  }

  @Override
  protected Batch getLocalOutOfOrderBatch() {
    DeviceSchema deviceSchema = getDeviceSchema();
    long loopIndex = insertLoop % config.getLOOP();
    Batch batch = new Batch();
    for (int i = 0; i < config.getBATCH_SIZE_PER_WRITE(); i++) {
      long stepOffset = loopIndex * config.getBATCH_SIZE_PER_WRITE() + i;
      if (config.isIS_SENSOR_TS_ALIGNMENT()) {
        addOneRowIntoBatch(batch, stepOffset);
      } else {
        addOneRowIntoBatch(batch, stepOffset, sensorIndex);
      }
    }
    batch.setDeviceSchema(deviceSchema);
    next();
    return batch;
  }

  private void next() {
    if (config.isIS_SENSOR_TS_ALIGNMENT()) {
      deviceIndex++;
    } else {
      sensorIndex++;
      if (sensorIndex >= deviceSchemas.get(deviceIndex).getSensors().size()) {
        deviceIndex++;
        sensorIndex = 0;
      }
    }
    if (deviceIndex >= deviceSchemaSize) {
      deviceIndex = 0;
      insertLoop++;
    }
  }
}
