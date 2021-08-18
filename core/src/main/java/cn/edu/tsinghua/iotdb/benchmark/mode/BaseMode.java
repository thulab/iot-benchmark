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
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public abstract class BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final double NANO_TO_SECOND = 1000000000.0d;

  /** Start benchmark */
  public abstract void run();

  /**
   * Save measure
   *
   * @param executorService
   * @param downLatch
   * @param measurement
   * @param threadsMeasurements
   * @param st
   * @param clients
   */
  protected static void finalMeasure(
          ExecutorService executorService,
          CountDownLatch downLatch,
          Measurement measurement,
          List<Measurement> threadsMeasurements,
          long st,
          List<Client> clients,
          List<Operation> operations) {
    executorService.shutdown();

    try {
      // wait for all clients finish test
      downLatch.await();
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all threads finish.", e);
      Thread.currentThread().interrupt();
    }
    long en = System.nanoTime();
    LOGGER.info("All clients finished.");
    // sum up all the measurements and calculate statistics
    measurement.setElapseTime((en - st) / NANO_TO_SECOND);
    for (Client client : clients) {
      threadsMeasurements.add(client.getMeasurement());
    }
    for (Measurement m : threadsMeasurements) {
      measurement.mergeMeasurement(m);
    }
    // must call calculateMetrics() before using the Metrics
    measurement.calculateMetrics(operations);
    // output results
    measurement.showConfigs();
    measurement.showMeasurements(operations);
    measurement.showMetrics(operations);
    if (config.isCSV_OUTPUT()) {
      measurement.outputCSV();
    }
  }
}
