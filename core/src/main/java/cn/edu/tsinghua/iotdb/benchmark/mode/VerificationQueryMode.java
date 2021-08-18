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
import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.client.real.RealDataSetQueryClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VerificationQueryMode extends BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(VerificationQueryMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

  /** Start benchmark */
  @Override
  public void run() {
    Measurement measurement = new Measurement();
    CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());

    // create getCLIENT_NUMBER() client threads to do the workloads
    List<Measurement> threadsMeasurements = new ArrayList<>();
    List<Client> clients = new ArrayList<>();
    CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
    long st = System.nanoTime();
    ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
    long loop = baseDataSchema.getLoopPerClient();

    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      Client client = new RealDataSetQueryClient(i, downLatch, barrier, loop);
      clients.add(client);
      executorService.submit(client);
    }
    finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients, new ArrayList<>(Arrays.asList(Operation.VERIFICATION_QUERY)));
  }
}
