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

import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;

import java.util.Map;

public interface ITestDataPersistence {

  /**
   * Store system resources metrics data
   *
   * @param systemMetricsMap System resources metrics to be stored
   */
  void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap);

  /** Save config of test */
  void saveTestConfig();

  /**
   * Save measurement result of operation
   *
   * @param operation which type of operation
   * @param okPoint okPoint of operation
   * @param failPoint failPoint of operation
   * @param latency latency of operation
   * @param remark remark of operation
   */
  void saveOperationResult(
      String operation, int okPoint, int failPoint, double latency, String remark);

  /**
   * Save result of operation
   *
   * @param operation
   * @param key
   * @param value
   */
  void saveResult(String operation, String key, String value);

  /** Close record */
  void close();
}
