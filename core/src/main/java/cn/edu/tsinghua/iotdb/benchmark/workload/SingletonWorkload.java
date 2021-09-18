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

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PoissonDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** The workload is in use when IS_CLIENT_BIND = false */
public class SingletonWorkload {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingletonWorkload.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private ProbTool probTool;
  private Random poissonRandom;
  private AtomicLong insertLoop;
  private ConcurrentHashMap<Integer, AtomicLong> deviceMaxTimeIndexMap;

  public static SingletonWorkload getInstance() {
    return SingletonWorkloadHolder.INSTANCE;
  }

  private SingletonWorkload() {
    insertLoop = new AtomicLong(0);
    deviceMaxTimeIndexMap = new ConcurrentHashMap<>();
    for (int i = 0; i < config.getDEVICE_NUMBER(); i++) {
      deviceMaxTimeIndexMap.put(MetaUtil.getDeviceId(i), new AtomicLong(0));
    }
    probTool = new ProbTool();
    poissonRandom = new Random(config.getDATA_SEED());
  }

  private Batch getOrderedBatch() {
    long curLoop = insertLoop.getAndIncrement();
    DeviceSchema deviceSchema =
        new DeviceSchema(
            MetaUtil.getDeviceId((int) curLoop % config.getDEVICE_NUMBER()),
            config.getSENSOR_CODES());
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      long stepOffset =
          (curLoop / config.getDEVICE_NUMBER()) * config.getBATCH_SIZE_PER_WRITE() + batchOffset;
      SyntheticDataWorkload.addOneRowIntoBatch(batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  public Batch getOneBatch() throws WorkloadException {
    if (!config.isIS_OUT_OF_ORDER()) {
      return getOrderedBatch();
    } else {
      switch (config.getOUT_OF_ORDER_MODE()) {
        case 0:
          return getDistOutOfOrderBatch();
        case 1:
          return getLocalOutOfOrderBatch();
        default:
          throw new WorkloadException(
              "Unsupported out of order mode: " + config.getOUT_OF_ORDER_MODE());
      }
    }
  }

  private Batch getDistOutOfOrderBatch() {
    long curLoop = insertLoop.getAndIncrement();
    int deviceIndex = MetaUtil.getDeviceId((int) (curLoop % config.getDEVICE_NUMBER()));
    DeviceSchema deviceSchema = new DeviceSchema(deviceIndex, config.getSENSOR_CODES());

    Batch batch = new Batch();
    PoissonDistribution poissonDistribution = new PoissonDistribution(poissonRandom);
    int nextDelta;
    long stepOffset;
    for (long batchOffset = 0; batchOffset < config.getBATCH_SIZE_PER_WRITE(); batchOffset++) {
      if (probTool.returnTrueByProb(config.getOUT_OF_ORDER_RATIO(), poissonRandom)) {
        // generate out of order timestamp
        nextDelta = poissonDistribution.getNextPoissonDelta();
        stepOffset = deviceMaxTimeIndexMap.get(deviceIndex).get() - nextDelta;
      } else {
        // generate normal increasing timestamp
        stepOffset = deviceMaxTimeIndexMap.get(deviceIndex).getAndIncrement();
      }
      SyntheticDataWorkload.addOneRowIntoBatch(batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  private Batch getLocalOutOfOrderBatch() {
    LOGGER.error("Not supported OUT_OF_ORDER_MODE = 1 when IS_CLIENT_BIND = false");
    return null;
  }

  private static class SingletonWorkloadHolder {
    private static final SingletonWorkload INSTANCE = new SingletonWorkload();
  }
}
