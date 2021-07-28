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

package cn.edu.tsinghua.iotdb.benchmark.measurement.enums;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;

import java.util.EnumMap;
import java.util.Map;

public enum Metric {
  AVG_LATENCY("AVG"),
  MIN_LATENCY("MIN"),
  P10_LATENCY("P10"),
  P25_LATENCY("P25"),
  MEDIAN_LATENCY("MEDIAN"),
  P75_LATENCY("P75"),
  P90_LATENCY("P90"),
  P95_LATENCY("P95"),
  P99_LATENCY("P99"),
  P999_LATENCY("P999"),
  MAX_LATENCY("MAX"),
  MAX_THREAD_LATENCY_SUM("SLOWEST_THREAD");

  public Map<Operation, Double> getTypeValueMap() {
    return typeValueMap;
  }

  public Map<Operation, Double> typeValueMap;

  public String getName() {
    return name;
  }

  public String name;

  Metric(String name) {
    this.name = name;
    typeValueMap = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      typeValueMap.put(operation, 0D);
    }
  }
}
