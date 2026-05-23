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

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Concurrency regression test for defect #3 — {@code SingletonWorkDataWorkLoad} pairing atomicity.
 *
 * <p>{@code getOneBatch()} draws the device cursor ({@code insertLoop}) and the sensor cursor from
 * two <em>independent</em> atomic counters. Each increment is atomic on its own, but the pairing
 * between them is not: under concurrency thread A can take device cursor {@code L} while thread B
 * races ahead and takes the sensor cursor that should have paired with {@code L}, so the {@code
 * (device, sensor)} mapping gets shuffled.
 *
 * <p>With {@code DEVICE_NUMBER == SENSOR_NUMBER} the correct mapping is a clean bijection: device
 * {@code p} is always paired with the same sensor (regardless of the counter's starting offset). So
 * the number of distinct {@code (device, colIndex)} pairs must equal the number of distinct devices
 * seen. Under the bug a device gets paired with several sensors and the pair count exceeds the
 * device count. This holds single-threaded for both the buggy and fixed code; only concurrency
 * breaks it, which is exactly what this test exercises.
 */
public class SingletonWorkDataWorkLoadConcurrencyTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /** DEVICE_NUMBER == SENSOR_NUMBER so the correct device→sensor mapping is a bijection. */
  private static final int SIZE = 200;

  private static final int THREADS = 16;
  private static final int CALLS_PER_THREAD = 600;

  private int origDeviceNumber;
  private boolean origSensorTsAlignment;
  private int origDeviceNumPerWrite;
  private int origBatchSize;
  private boolean origOutOfOrder;
  private int origDataClientNumber;
  private int origSchemaClientNumber;

  @Before
  public void setUp() {
    origDeviceNumber = config.getDEVICE_NUMBER();
    origSensorTsAlignment = config.isIS_SENSOR_TS_ALIGNMENT();
    origDeviceNumPerWrite = config.getDEVICE_NUM_PER_WRITE();
    origBatchSize = config.getBATCH_SIZE_PER_WRITE();
    origOutOfOrder = config.isIS_OUT_OF_ORDER();
    origDataClientNumber = config.getDATA_CLIENT_NUMBER();
    origSchemaClientNumber = config.getSCHEMA_CLIENT_NUMBER();

    // SENSOR_NUMBER stays at its default (200) so the pre-populated SENSORS list is consistent.
    config.setDEVICE_NUMBER(SIZE);
    // Force the buggy else-branch that pairs a device with a single sensor cursor.
    config.setIS_SENSOR_TS_ALIGNMENT(false);
    // One device per batch → one colIndex per batch, the unit we collect.
    config.setDEVICE_NUM_PER_WRITE(1);
    // Keep each getOneBatch() cheap; we care about the (device, sensor) pairing, not the records.
    config.setBATCH_SIZE_PER_WRITE(1);
    config.setIS_OUT_OF_ORDER(false);
    // Keep the singletons that DataWorkLoad eagerly builds consistent with the end-to-end test.
    config.setDATA_CLIENT_NUMBER(1);
    config.setSCHEMA_CLIENT_NUMBER(1);
  }

  @After
  public void tearDown() {
    config.setDEVICE_NUMBER(origDeviceNumber);
    config.setIS_SENSOR_TS_ALIGNMENT(origSensorTsAlignment);
    config.setDEVICE_NUM_PER_WRITE(origDeviceNumPerWrite);
    config.setBATCH_SIZE_PER_WRITE(origBatchSize);
    config.setIS_OUT_OF_ORDER(origOutOfOrder);
    config.setDATA_CLIENT_NUMBER(origDataClientNumber);
    config.setSCHEMA_CLIENT_NUMBER(origSchemaClientNumber);
  }

  @Test
  public void devicePairsWithExactlyOneSensorUnderConcurrency() throws Exception {
    SingletonWorkDataWorkLoad workload = SingletonWorkDataWorkLoad.getInstance();

    Set<String> distinctDevices = ConcurrentHashMap.newKeySet();
    Set<String> distinctPairs = ConcurrentHashMap.newKeySet();
    CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();

    CyclicBarrier startLine = new CyclicBarrier(THREADS);
    CountDownLatch done = new CountDownLatch(THREADS);
    for (int t = 0; t < THREADS; t++) {
      new Thread(
              () -> {
                try {
                  startLine.await();
                  for (int i = 0; i < CALLS_PER_THREAD; i++) {
                    IBatch batch = workload.getOneBatch();
                    String device = batch.getDeviceSchema().getDevice();
                    int sensor = batch.getColIndex();
                    distinctDevices.add(device);
                    distinctPairs.add(device + "#" + sensor);
                  }
                } catch (Throwable e) {
                  failures.add(e);
                } finally {
                  done.countDown();
                }
              })
          .start();
    }
    done.await();

    assertTrue("getOneBatch threw under concurrency: " + failures, failures.isEmpty());
    // Sanity: enough calls actually hit a spread of devices, otherwise the assertion is vacuous.
    assertTrue(
        "expected to exercise many devices but only saw " + distinctDevices.size(),
        distinctDevices.size() > 1);
    assertEquals(
        "each device must pair with exactly one sensor; more pairs than devices means the device "
            + "and sensor cursors were mismatched by a concurrent caller (defect #3)",
        distinctDevices.size(),
        distinctPairs.size());
  }
}
