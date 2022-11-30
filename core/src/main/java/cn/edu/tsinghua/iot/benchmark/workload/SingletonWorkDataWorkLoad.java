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

package cn.edu.tsinghua.iot.benchmark.workload;

import cn.edu.tsinghua.iot.benchmark.distribution.PoissonDistribution;
import cn.edu.tsinghua.iot.benchmark.entity.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SingletonWorkDataWorkLoad extends GenerateDataWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingletonWorkDataWorkLoad.class);
  private static final List<Sensor> SENSORS = Collections.synchronizedList(config.getSENSORS());
  private ConcurrentHashMap<Integer, AtomicLong> deviceMaxTimeIndexMap;
  private static SingletonWorkDataWorkLoad singletonWorkDataWorkLoad = null;
  private static AtomicInteger sensorIndex = new AtomicInteger();
  private AtomicLong insertLoop = new AtomicLong(0);

  private SingletonWorkDataWorkLoad() {
    if (config.isIS_OUT_OF_ORDER()) {
      long startIndex = (long) (config.getLOOP() * config.getOUT_OF_ORDER_RATIO());
      this.insertLoop.set(startIndex);
    }
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
    Batch batch = getBatchWithDeviceSchema(curLoop);
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      long stepOffset =
          (curLoop / config.getDEVICE_NUMBER()) * config.getBATCH_SIZE_PER_WRITE() + batchOffset;
      addOneRowIntoBatch(batch, stepOffset);
    }
    return batch;
  }

  @Override
  protected Batch getDistOutOfOrderBatch() {
    long curLoop = insertLoop.getAndIncrement();
    Batch batch = getBatchWithDeviceSchema(curLoop);
    int deviceId = batch.getDeviceSchema().getDeviceId();
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
      addOneRowIntoBatch(batch, stepOffset);
    }
    return batch;
  }

  private Batch getBatchWithDeviceSchema(long loop) {
    Batch batch = new Batch();
    List<Sensor> sensors = new ArrayList<>();
    if (config.isIS_SENSOR_TS_ALIGNMENT()) {
      sensors = SENSORS;
    } else {
      int sensorId = sensorIndex.getAndIncrement() % config.getSENSOR_NUMBER();
      batch.setColIndex(sensorId);
      sensors.add(SENSORS.get(sensorId));
    }
    DeviceSchema deviceSchema =
        new DeviceSchema(
            MetaUtil.getDeviceId((int) loop % config.getDEVICE_NUMBER()),
            sensors,
            config.getDEVICE_TAGS());
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  @Override
  protected Batch getLocalOutOfOrderBatch() {
    long loopIndex = insertLoop.getAndIncrement() % config.getLOOP();
    Batch batch = getBatchWithDeviceSchema(loopIndex);
    for (int i = 0; i < config.getBATCH_SIZE_PER_WRITE(); i++) {
      long stepOffset = loopIndex * config.getBATCH_SIZE_PER_WRITE() + i;
      addOneRowIntoBatch(batch, stepOffset);
    }
    return batch;
  }
}
