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

import cn.edu.tsinghua.iot.benchmark.entity.Batch.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.MultiDeviceBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SingletonWorkDataWorkLoad extends GenerateDataWorkLoad {
  private static final List<Sensor> SENSORS = Collections.synchronizedList(config.getSENSORS());
  private static SingletonWorkDataWorkLoad singletonWorkDataWorkLoad = null;
  private static final AtomicInteger sensorIndex = new AtomicInteger();
  private final AtomicLong insertLoop = new AtomicLong(0);

  private SingletonWorkDataWorkLoad() {
    if (config.isIS_OUT_OF_ORDER()) {
      long startIndex = (long) (config.getLOOP() * config.getOUT_OF_ORDER_RATIO());
      this.insertLoop.set(startIndex);
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
  public IBatch getOneBatch() throws WorkloadException {
    IBatch batch = null;
    // TODO: bad, should be fixed in the future
    final int recordsNumPerDevice = config.getBATCH_SIZE_PER_WRITE();
    if (config.getDEVICE_NUM_PER_WRITE() == 1) {
      batch = new Batch();
    } else {
      batch = new MultiDeviceBatch(config.getDEVICE_NUM_PER_WRITE());
    }
    for (int i = 0; i < config.getDEVICE_NUM_PER_WRITE(); i++) {
      long curLoop = insertLoop.getAndIncrement();
      // create schema of batch
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
              MetaUtil.getDeviceId((int) curLoop % config.getDEVICE_NUMBER()),
              sensors,
              MetaUtil.getTags((int) curLoop % config.getDEVICE_NUMBER()));
      // create data of batch
      List<Record> records = new ArrayList<>();
      for (long batchOffset = 0; batchOffset < recordsNumPerDevice; batchOffset++) {
        long stepOffset =
            (curLoop / config.getDEVICE_NUMBER()) * config.getBATCH_SIZE_PER_WRITE() + batchOffset;
        records.add(
            new Record(
                getCurrentTimestamp(stepOffset),
                generateOneRow(deviceSchema.getDeviceId(), batch.getColIndex(), stepOffset)));
      }
      batch.addSchemaAndContent(deviceSchema, records);
    }
    return batch;
  }
}
