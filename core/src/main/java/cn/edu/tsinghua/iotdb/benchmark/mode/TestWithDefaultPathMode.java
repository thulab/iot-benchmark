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

package cn.edu.tsinghua.iotdb.benchmark.mode;

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.client.generate.SyntheticClient;
import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.TestDataPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestWithDefaultPathMode extends BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestWithDefaultPathMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /** Start benchmark */
  @Override
  public void run() {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    TestDataPersistence recorder = persistenceFactory.getPersistence();
    recorder.saveTestConfig();

    Measurement measurement = new Measurement();
    registerSchema(config.getDbConfig(), measurement);
    if (config.isIS_DOUBLE_WRITE()) {
      registerSchema(config.getANOTHER_DBConfig(), measurement);
    }

    // create getCLIENT_NUMBER() client threads to do the workloads
    List<Measurement> threadsMeasurements = new ArrayList<>();
    List<Client> clients = new ArrayList<>();
    CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
    CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());
    long st = 0;
    ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
    LOGGER.info("Generating workload buffer...");
    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      SyntheticClient client = new SyntheticClient(i, downLatch, barrier);
      clients.add(client);
      st = System.nanoTime();
      executorService.submit(client);
    }
    List<Operation> operations = new ArrayList<>();
    if (!config.isIS_POINT_COMPARISON()) {
      operations = Operation.getNormalOperation();
    }
    finalMeasure(
        executorService, downLatch, measurement, threadsMeasurements, st, clients, operations);
  }
}
