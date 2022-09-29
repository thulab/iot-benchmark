/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
          DeviceSchema deviceSchema =
              new DeviceSchema(
                  schema.getDeviceId(), Collections.singletonList(sensor), config.getDEVICE_TAGS());
          maxTimestampIndexMap.put(deviceSchema, 0L);
        }
      }
    }
    this.deviceSchemaSize = deviceSchemas.size();
  }

  @Override
  protected Batch getOrderedBatch() {
    Batch batch = getBatchWithSchema();
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      long stepOffset = insertLoop * config.getBATCH_SIZE_PER_WRITE() + batchOffset;
      stepOffset += Math.exp(dataRandom.nextGaussian());
      addOneRowIntoBatch(batch, stepOffset);
    }
    next();
    return batch;
  }

  private Batch getBatchWithSchema() {
    Batch batch = new Batch();
    DeviceSchema deviceSchema =
        new DeviceSchema(
            deviceSchemas.get(deviceIndex).getDeviceId(),
            deviceSchemas.get(deviceIndex).getSensors(),
            deviceSchemas.get(deviceIndex).getTags());
    if (!config.isIS_SENSOR_TS_ALIGNMENT()) {
      List<Sensor> sensors = new ArrayList<>();
      sensors.add(deviceSchema.getSensors().get(sensorIndex));
      deviceSchema.setSensors(sensors);
      batch.setColIndex(sensorIndex);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  @Override
  protected Batch getDistOutOfOrderBatch() {
    Batch batch = getBatchWithSchema();
    DeviceSchema deviceSchema = batch.getDeviceSchema();
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
      addOneRowIntoBatch(batch, stepOffset);
    }
    next();
    return batch;
  }

  @Override
  protected Batch getLocalOutOfOrderBatch() {
    long loopIndex = insertLoop % config.getLOOP();
    Batch batch = getBatchWithSchema();
    for (int i = 0; i < config.getBATCH_SIZE_PER_WRITE(); i++) {
      long stepOffset = loopIndex * config.getBATCH_SIZE_PER_WRITE() + i;
      addOneRowIntoBatch(batch, stepOffset);
    }
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
