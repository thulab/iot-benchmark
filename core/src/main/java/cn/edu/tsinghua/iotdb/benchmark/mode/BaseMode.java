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
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final double NANO_TO_SECOND = 1000000000.0d;

  protected ExecutorService executorService =
      Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
  protected CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
  protected CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());
  protected List<Client> clients = new ArrayList<>();
  protected Measurement measurement = new Measurement();
  protected long start = 0;

  protected abstract boolean preCheck();

  /** Start benchmark */
  public void run() {
    if (!preCheck()) {
      return;
    }
    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      Client client = Client.getInstance(i, downLatch, barrier);
      if (client == null) {
        return;
      }
      clients.add(client);
      start = System.nanoTime();
      executorService.submit(client);
    }
    executorService.shutdown();
    try {
      // wait for all clients finish test
      downLatch.await();
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all threads finish.", e);
      Thread.currentThread().interrupt();
    }
    postCheck();
  }

  protected abstract void postCheck();

  /** Register schema */
  protected void registerSchema(List<DBConfig> dbConfigs, Measurement measurement) {
    DBWrapper dbWrapper = new DBWrapper(dbConfigs, measurement);
    // register schema if needed
    try {
      dbWrapper.init();
      if (config.isIS_DELETE_DATA()) {
        try {
          dbWrapper.cleanup();
        } catch (TsdbException e) {
          LOGGER.error("Cleanup {} failed because ", config.getNET_DEVICE(), e);
        }
      }
      try {
        MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
        List<DeviceSchema> schemaList = metaDataSchema.getAllDeviceSchemas();
        dbWrapper.registerSchema(schemaList);
      } catch (TsdbException e) {
        LOGGER.error("Register {} schema failed because ", config.getNET_DEVICE(), e);
      }
    } catch (TsdbException e) {
      LOGGER.error("Initialize {} failed because ", config.getNET_DEVICE(), e);
    } finally {
      try {
        dbWrapper.close();
      } catch (TsdbException e) {
        LOGGER.error("Close {} failed because ", config.getNET_DEVICE(), e);
      }
    }
  }

  /** Save measure */
  protected static void finalMeasure(
      Measurement measurement,
      List<Measurement> threadsMeasurements,
      long st,
      List<Client> clients,
      List<Operation> operations) {
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
    if (operations.size() != 0) {
      measurement.showMeasurements(operations);
      measurement.showMetrics(operations);
    }
    if (config.isCSV_OUTPUT()) {
      measurement.outputCSV();
    }
  }
}
