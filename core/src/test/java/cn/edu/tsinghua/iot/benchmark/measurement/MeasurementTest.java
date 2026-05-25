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

package cn.edu.tsinghua.iot.benchmark.measurement;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.measurement.enums.Metric;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MeasurementTest extends BenchmarkTestBase {

  private static final double DELTA = 1e-9;

  /**
   * Regression test for the AVG_LATENCY accumulation bug (issue #1).
   *
   * <p>{@code BaseMode.measure()} performs {@code resetMeasurementMaps() + mergeMeasurement() +
   * calculateMetrics()} on every periodic (middle) measurement. With the same underlying client
   * data, two consecutive measurements must yield the same AVG_LATENCY. Previously {@code
   * operationLatencySumAllClient} was static and never reset, so the second measurement doubled the
   * numerator while the denominator stayed correct, inflating AVG_LATENCY.
   */
  @Test
  public void testAvgLatencyNotAccumulatedAcrossMeasurements() {
    Operation op = Operation.PRECISE_QUERY;
    List<Operation> operations = Collections.singletonList(op);

    // one client with two operations: latencies 10ms and 20ms -> sum 30, count 2, avg 15
    Measurement client = new Measurement();
    client.addOperationLatency(op, 10.0);
    client.addOkOperationNum(op);
    client.addOperationLatency(op, 20.0);
    client.addOkOperationNum(op);

    Measurement aggregator = new Measurement();

    double avgFirst = measureAndGetAvg(aggregator, client, operations, op);
    double avgSecond = measureAndGetAvg(aggregator, client, operations, op);

    assertEquals(
        "AVG_LATENCY must not change when client data is unchanged", avgFirst, avgSecond, DELTA);
    assertEquals(15.0, avgSecond, DELTA);
  }

  /** Mirrors the reset + merge + calculate flow that {@code BaseMode.measure()} runs each time. */
  private double measureAndGetAvg(
      Measurement aggregator, Measurement client, List<Operation> operations, Operation op) {
    aggregator.resetMeasurementMaps();
    aggregator.mergeMeasurement(client);
    aggregator.calculateMetrics(operations);
    return Metric.AVG_LATENCY.getTypeValueMap().get(op);
  }
}
