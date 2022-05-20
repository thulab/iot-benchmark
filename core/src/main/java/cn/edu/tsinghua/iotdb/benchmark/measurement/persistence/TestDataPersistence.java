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

package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class TestDataPersistence {

  protected static final Logger LOGGER = LoggerFactory.getLogger(TestDataPersistence.class);
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected ExecutorService service =
      Executors.newFixedThreadPool(config.getTEST_DATA_MAX_CONNECTION());

  /**
   * Store system resources metrics data
   *
   * @param systemMetricsMap System resources metrics to be stored
   */
  public abstract void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap);

  /** Save config of test */
  public abstract void saveTestConfig();

  /**
   * Save measurement result of operation
   *
   * @param operation which sensorType of operation
   * @param okPoint okPoint of operation
   * @param failPoint failPoint of operation
   * @param latency latency of operation
   * @param remark remark of operation
   * @param device
   */
  protected abstract void saveOperationResult(
      String operation, int okPoint, int failPoint, double latency, String remark, String device);

  /** Create new record when line meet max line */
  protected abstract void createNewRecord(
      String operation, int okPoint, int failPoint, double latency, String remark, String device);

  /**
   * Save result of operation
   *
   * @param operation
   * @param key
   * @param value
   */
  protected abstract void saveResult(String operation, String key, String value);

  /**
   * Save measurement result of operation async
   *
   * @param operation which sensorType of operation
   * @param okPoint okPoint of operation
   * @param failPoint failPoint of operation
   * @param latency latency of operation
   * @param remark remark of operation
   */
  public void saveOperationResultAsync(
      String operation, int okPoint, int failPoint, double latency, String remark, String device) {
    service.submit(
        () -> {
          saveOperationResult(operation, okPoint, failPoint, latency, remark, device);
        });
  }

  /** Save result of operation Async */
  public void saveResultAsync(String operation, String key, String value) {
    service.submit(
        () -> {
          saveResult(operation, key, value);
        });
  }

  /** Close record */
  protected abstract void close();

  public void closeAsync() {
    while (!service.isTerminated()) {
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        // do nothing
      }
      close();
      service.shutdown();
    }
  }
}
