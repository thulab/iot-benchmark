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

package cn.edu.tsinghua.iot.benchmark.tsdb;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.fakedb.FakeDB;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.DeviceQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Regression tests for the {@link DBWrapper} correctness defects #8/#9/#11/#12 from the core
 * review.
 *
 * <p>{@code DBWrapper.databases} is private and only populated through {@link DBFactory}
 * reflection, so these tests inject {@link FakeDB}-based doubles via the {@link DBWrapper#forTest}
 * seam and read the per-operation counters back from {@link DBWrapper#getMeasurement()}.
 */
public class DBWrapperTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private boolean originalUseMeasurement;

  @Before
  public void enableMeasurement() {
    // The wrapper only records operation counters when measurement is enabled.
    originalUseMeasurement = config.isUSE_MEASUREMENT();
    config.setUSE_MEASUREMENT(true);
  }

  @After
  public void restoreConfig() {
    config.setUSE_MEASUREMENT(originalUseMeasurement);
  }

  /**
   * #8 — A database that cannot be initialized must abort the run, not leave {@code databases}
   * empty so the benchmark "succeeds" while writing nothing.
   *
   * <p>The default {@link DBConfig} points at an IoTDB switch whose adapter class lives in a
   * separate module and is absent from the core test classpath, so {@link DBFactory#getDatabase}
   * fails for it.
   */
  @Test
  public void constructionFailureMustNotBeSilent() {
    List<DBConfig> configs = Collections.singletonList(new DBConfig());
    try {
      new DBWrapper(configs);
      fail(
          "DBWrapper must fail fast when a database cannot be initialized; otherwise it runs "
              + "against an empty database list and silently writes nothing");
    } catch (IllegalStateException expected) {
      // expected once #8 is fixed
    }
  }

  /**
   * #9 — {@code aggRangeValueQuery} measured the query outside the per-database loop, counting only
   * the last database. Every other query method counts once per database; this asserts the parity.
   */
  @Test
  public void aggRangeValueQueryCountsEveryDatabaseLikeItsSiblings() {
    List<IDatabase> twoDatabases = Arrays.<IDatabase>asList(new FakeDB(), new FakeDB());
    DBWrapper wrapper = DBWrapper.forTest(twoDatabases);

    wrapper.aggRangeQuery(new AggRangeQuery(new ArrayList<>(), 0L, 1L, "avg"));
    wrapper.aggRangeValueQuery(new AggRangeValueQuery(new ArrayList<>(), 0L, 1L, "avg", 0.0));

    Measurement measurement = wrapper.getMeasurement();
    assertEquals(
        "sanity: the correct sibling counts once per database",
        2L,
        measurement.getOkOperationNum(Operation.AGG_RANGE_QUERY));
    assertEquals(
        "aggRangeValueQuery must count once per database, like every other query method",
        measurement.getOkOperationNum(Operation.AGG_RANGE_QUERY),
        measurement.getOkOperationNum(Operation.AGG_RANGE_VALUE_QUERY));
  }

  /**
   * #11 — When the last database returns a null summary, the original code dereferenced the stale
   * {@code deviceSummary} reference outside the try/catch and threw {@link NullPointerException}.
   * The agreed-upon summary from the databases that did answer must be returned instead.
   */
  @Test
  public void deviceSummaryMustNotThrowWhenSomeDatabasesReturnNull() throws Exception {
    DeviceSummary summary = new DeviceSummary("d_0", 10, 0L, 100L);
    List<IDatabase> databases =
        Arrays.<IDatabase>asList(new FixedSummaryDB(summary), new FixedSummaryDB(null));
    DBWrapper wrapper = DBWrapper.forTest(databases);

    assertEquals(summary, wrapper.deviceSummary(new DeviceQuery()));
  }

  /** #11 — With no usable summary at all, the wrapper must return null gracefully, not crash. */
  @Test
  public void deviceSummaryReturnsNullWhenAllDatabasesReturnNull() throws Exception {
    List<IDatabase> databases =
        Arrays.<IDatabase>asList(new FixedSummaryDB(null), new FixedSummaryDB(null));
    DBWrapper wrapper = DBWrapper.forTest(databases);

    assertNull(wrapper.deviceSummary(new DeviceQuery()));
  }

  /**
   * #12 — {@code doPointComparison} indexed {@code statuses.get(1)} with no size guard, so a single
   * configured database made every device query throw and be recorded as a failure. With one
   * database the comparison must be skipped and the query counted as a success.
   */
  @Test
  public void deviceQueryWithSingleDatabaseIsNotCountedAsFailure() throws Exception {
    List<IDatabase> singleDatabase = Collections.<IDatabase>singletonList(new RecordReturningDB());
    DBWrapper wrapper = DBWrapper.forTest(singleDatabase);

    DeviceSchema deviceSchema = new DeviceSchema();
    deviceSchema.setDevice("d_0");
    wrapper.deviceQuery(new DeviceQuery(deviceSchema));

    Measurement measurement = wrapper.getMeasurement();
    assertEquals(
        "single-database device query must not be recorded as a failure",
        0L,
        measurement.getFailOperationNum(Operation.DEVICE_QUERY));
    assertEquals(1L, measurement.getOkOperationNum(Operation.DEVICE_QUERY));
  }

  /** Returns a preset (possibly null) device summary, ignoring the query. */
  private static class FixedSummaryDB extends FakeDB {
    private final DeviceSummary summary;

    FixedSummaryDB(DeviceSummary summary) {
      this.summary = summary;
    }

    @Override
    public DeviceSummary deviceSummary(DeviceQuery deviceQuery) {
      return summary;
    }
  }

  /** Returns a successful device query with a single record row. */
  private static class RecordReturningDB extends FakeDB {
    @Override
    public Status deviceQuery(DeviceQuery deviceQuery) {
      List<List<Object>> records = new ArrayList<>();
      List<Object> row = new ArrayList<>();
      row.add(0L);
      row.add(1);
      records.add(row);
      return new Status(true, 1, "fake-sql", records);
    }
  }
}
