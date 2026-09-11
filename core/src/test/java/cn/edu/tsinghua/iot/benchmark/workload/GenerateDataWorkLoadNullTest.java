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
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.workload.interfaces.IDataWorkLoad;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

/**
 * Tests for the sparse matrix write feature ({@code NULL_RATIO}): {@code
 * GenerateDataWorkLoad.generateOneRow} turns each cell of a row into null with the configured
 * probability, independently per column, seeded by {@code DATA_SEED} so the pattern is
 * deterministic.
 */
public class GenerateDataWorkLoadNullTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /** Number of sensors per device; must match {@code config.getSENSOR_NUMBER()}. */
  private static final int SENSOR_NUMBER = config.getSENSOR_NUMBER();

  private static final int BATCHES = 200;
  private static final int ROWS_PER_BATCH = 10;
  private static final int THREADS = 8;
  private static final int BATCHES_PER_THREAD = 200;
  private static final double RATIO = 0.9;
  private static final double TOLERANCE = 0.05;

  private double origNullRatio;
  private boolean origSensorTsAlignment;
  private int origDeviceNumPerWrite;
  private int origBatchSize;
  private boolean origOutOfOrder;
  private String origOperationProportion;

  @Before
  public void setUp() {
    origNullRatio = config.getNULL_RATIO();
    origSensorTsAlignment = config.isIS_SENSOR_TS_ALIGNMENT();
    origDeviceNumPerWrite = config.getDEVICE_NUM_PER_WRITE();
    origBatchSize = config.getBATCH_SIZE_PER_WRITE();
    origOutOfOrder = config.isIS_OUT_OF_ORDER();
    origOperationProportion = config.getOPERATION_PROPORTION();

    // Write must be enabled when GenerateDataWorkLoad builds its static workloadValues,
    // otherwise the buffer stays null and getOneBatch() NPEs.
    config.setOPERATION_PROPORTION("1:0:0:0:0:0:0:0:0:0:0:0:0");
    config.setDEVICE_NUM_PER_WRITE(1);
    config.setBATCH_SIZE_PER_WRITE(ROWS_PER_BATCH);
    config.setIS_OUT_OF_ORDER(false);
  }

  @After
  public void tearDown() {
    config.setNULL_RATIO(origNullRatio);
    config.setIS_SENSOR_TS_ALIGNMENT(origSensorTsAlignment);
    config.setDEVICE_NUM_PER_WRITE(origDeviceNumPerWrite);
    config.setBATCH_SIZE_PER_WRITE(origBatchSize);
    config.setIS_OUT_OF_ORDER(origOutOfOrder);
    config.setOPERATION_PROPORTION(origOperationProportion);
  }

  /** Two devices whose schemas carry all {@code SENSOR_NUMBER} sensors. */
  private List<DeviceSchema> twoDevices() {
    List<Sensor> sensors = new ArrayList<>(config.getSENSORS());
    Map<String, String> tags = Collections.emptyMap();
    return Arrays.asList(new DeviceSchema(0, sensors, tags), new DeviceSchema(1, sensors, tags));
  }

  private SyntheticDataWorkLoad newWorkload() {
    return new SyntheticDataWorkLoad(twoDevices());
  }

  /** Collects every value of every record of the batches into a flat list. */
  private List<Object> flattenValues(SyntheticDataWorkLoad workload, int batches) throws Exception {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < batches; i++) {
      IBatch batch = workload.getOneBatch();
      for (Record record : batch.getRecords()) {
        values.addAll(record.getRecordDataValue());
      }
    }
    return values;
  }

  @Test
  public void nullRatioDisabledProducesNoNulls() throws Exception {
    config.setNULL_RATIO(0);
    config.setIS_SENSOR_TS_ALIGNMENT(true);

    List<Object> values = flattenValues(newWorkload(), BATCHES);
    assertTrue("expected " + values.size() + " non-null values", !values.isEmpty());
    for (Object value : values) {
      assertTrue("NULL_RATIO=0 must not produce null values", value != null);
    }
  }

  @Test
  public void nullRatioApproximatesConfiguredProbability() throws Exception {
    config.setNULL_RATIO(RATIO);
    config.setIS_SENSOR_TS_ALIGNMENT(true);

    List<Object> values = flattenValues(newWorkload(), BATCHES);
    int nullCount = 0;
    for (Object value : values) {
      if (value == null) {
        nullCount++;
      }
    }
    double actual = (double) nullCount / values.size();
    assertEquals(
        "observed null fraction should be close to NULL_RATIO=" + RATIO, RATIO, actual, TOLERANCE);
  }

  @Test
  public void nullPatternIsDeterministic() throws Exception {
    config.setNULL_RATIO(RATIO);
    config.setIS_SENSOR_TS_ALIGNMENT(true);

    SyntheticDataWorkLoad first = newWorkload();
    SyntheticDataWorkLoad second = newWorkload();
    int rows = 0;
    for (int i = 0; i < BATCHES; i++) {
      List<Record> recordsA = first.getOneBatch().getRecords();
      List<Record> recordsB = second.getOneBatch().getRecords();
      assertEquals(recordsA.size(), recordsB.size());
      for (int r = 0; r < recordsA.size(); r++) {
        List<Object> valuesA = recordsA.get(r).getRecordDataValue();
        List<Object> valuesB = recordsB.get(r).getRecordDataValue();
        assertEquals(valuesA.size(), valuesB.size());
        for (int v = 0; v < valuesA.size(); v++) {
          assertEquals(
              "same DATA_SEED must produce the same null pattern",
              valuesA.get(v) == null,
              valuesB.get(v) == null);
          rows++;
        }
      }
    }
    assertTrue("expected to compare many values but only saw " + rows, rows > 10000);
  }

  @Test
  public void nonAlignedModeAppliesPerValueProbability() throws Exception {
    config.setNULL_RATIO(RATIO);
    config.setIS_SENSOR_TS_ALIGNMENT(false);
    // One device, one row, one value per batch: each generated row holds a single value.
    config.setDEVICE_NUM_PER_WRITE(1);
    config.setBATCH_SIZE_PER_WRITE(1);

    int nullCount = 0;
    int valueCount = 0;
    SyntheticDataWorkLoad workload = newWorkload();
    for (int i = 0; i < 4000; i++) {
      IBatch batch = workload.getOneBatch();
      assertEquals(1, batch.getRecords().size());
      List<Object> row = batch.getRecords().get(0).getRecordDataValue();
      assertEquals("non-aligned mode generates one value per row", 1, row.size());
      if (row.get(0) == null) {
        nullCount++;
      }
      valueCount++;
    }
    assertEquals(
        "non-aligned mode applies NULL_RATIO as a per-value probability",
        RATIO,
        (double) nullCount / valueCount,
        TOLERANCE);
  }

  @Test
  public void sharedWorkloadBufferIsNotMutatedByNulls() throws Exception {
    config.setNULL_RATIO(RATIO);
    config.setIS_SENSOR_TS_ALIGNMENT(true);

    // Generate rows with nulls; generateOneRow copies values into a fresh list per row.
    flattenValues(newWorkload(), BATCHES);

    Field field = GenerateDataWorkLoad.class.getDeclaredField("workloadValues");
    field.setAccessible(true);
    Object[][] buffer = (Object[][]) field.get(null);
    // The static buffer is null when the class was loaded without a write workload.
    assumeNotNull(buffer);
    for (int sensor = 0; sensor < buffer.length; sensor++) {
      for (int i = 0; i < buffer[sensor].length; i++) {
        assertTrue(
            "the shared workload buffer must never contain nulls", buffer[sensor][i] != null);
      }
    }
  }

  /**
   * The null pattern must not depend on how threads interleave. {@code SingletonWorkDataWorkLoad}
   * is shared by every data client when {@code IS_CLIENT_BIND=false}, so a null decision drawn from
   * a shared {@link java.util.Random} would depend on which thread happens to consume the next
   * random number, and the same run would produce different null positions every time. The decision
   * is derived from {@code DATA_SEED} plus the cell coordinates instead.
   *
   * <p>Two concurrent passes are compared cell by cell, keyed by {@code (device, timestamp,
   * column)}: every coordinate generated by both passes must agree on whether it is null.
   */
  @Test
  public void nullPatternIsReproducibleAcrossConcurrentWriters() throws Exception {
    config.setNULL_RATIO(RATIO);
    config.setIS_SENSOR_TS_ALIGNMENT(true);

    Map<String, Boolean> firstPass = generateConcurrently(THREADS, BATCHES_PER_THREAD);
    Map<String, Boolean> secondPass = generateConcurrently(THREADS, BATCHES_PER_THREAD);

    int compared = 0;
    int differing = 0;
    for (Map.Entry<String, Boolean> entry : firstPass.entrySet()) {
      Boolean other = secondPass.get(entry.getKey());
      if (other == null) {
        continue;
      }
      compared++;
      if (!other.equals(entry.getValue())) {
        differing++;
      }
    }
    assertTrue(
        "expected the two passes to share many coordinates, but only " + compared + " matched",
        compared > 1000);
    assertEquals(
        "the same seed must place nulls at the same (device, timestamp, column) coordinates no "
            + "matter how the writer threads interleave",
        0,
        differing);
  }

  /**
   * Resets the {@code SingletonWorkDataWorkLoad} singleton so the next pass starts from the same
   * {@code insertLoop} and therefore revisits the same coordinates.
   */
  private static void resetSingleton() throws Exception {
    Field field = SingletonWorkDataWorkLoad.class.getDeclaredField("singletonWorkDataWorkLoad");
    field.setAccessible(true);
    field.set(null, null);
  }

  /**
   * Generates in {@code threads} concurrent clients and records each cell's null-ness by
   * coordinate.
   */
  private Map<String, Boolean> generateConcurrently(int threads, int batchesPerThread)
      throws Exception {
    resetSingleton();
    IDataWorkLoad workload = SingletonWorkDataWorkLoad.getInstance();
    Map<String, Boolean> nullByCoordinate = new ConcurrentHashMap<>();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(
            pool.submit(
                () -> {
                  try {
                    for (int i = 0; i < batchesPerThread; i++) {
                      recordBatch(workload.getOneBatch(), nullByCoordinate);
                    }
                  } catch (WorkloadException e) {
                    throw new IllegalStateException(e);
                  }
                  return null;
                }));
      }
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      pool.shutdownNow();
    }
    return nullByCoordinate;
  }

  private static void recordBatch(IBatch batch, Map<String, Boolean> nullByCoordinate) {
    batch.reset();
    while (true) {
      String device = batch.getDeviceSchema().getDevice();
      for (Record record : batch.getRecords()) {
        List<Object> values = record.getRecordDataValue();
        for (int column = 0; column < values.size(); column++) {
          nullByCoordinate.put(
              device + "|" + record.getTimestamp() + "|" + column, values.get(column) == null);
        }
      }
      if (!batch.hasNext()) {
        break;
      }
      batch.next();
    }
  }
}
