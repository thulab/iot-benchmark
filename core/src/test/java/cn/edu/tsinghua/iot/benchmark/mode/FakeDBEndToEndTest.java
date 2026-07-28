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

package cn.edu.tsinghua.iot.benchmark.mode;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBSwitch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end smoke test of the whole {@link BaseMode} pipeline (register schema → write → read →
 * aggregate) driven against {@link cn.edu.tsinghua.iot.benchmark.tsdb.fakedb.FakeDB}, so no real
 * database is required.
 *
 * <p>This is the regression net for the "ran to completion but actually wrote/read nothing" class
 * of bugs (#1, #8): FakeDB returns success for every operation, so a healthy run must end with
 * non-zero ok operation/point counts and zero failures. An empty database list, a swallowed init
 * failure, or a measurement that never accumulates would all show up here as zero counts.
 *
 * <p>FakeDB itself only became loadable through {@code DBFactory} once it gained a {@code
 * (DBConfig)} constructor, so this test also exercises that wiring.
 */
public class FakeDBEndToEndTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /**
   * DEVICE_NUMBER == default SENSOR_NUMBER keeps the workload singletons consistent across tests.
   */
  private static final int DEVICES = 200;

  private DBSwitch origDbSwitch;
  private String origPersistence;
  private boolean origUseMeasurement;
  private int origDeviceNumber;
  private int origDataClientNumber;
  private int origSchemaClientNumber;
  private long origLoop;
  private int origBatchSize;
  private boolean origCreateSchema;
  private boolean origDeleteData;
  private boolean origClientBind;
  private String origOperationProportion;
  private int origResultPrintInterval;
  private Long origTestMaxTime;

  @Before
  public void setUp() {
    origDbSwitch = config.getDbConfig().getDB_SWITCH();
    origPersistence = config.getTEST_DATA_PERSISTENCE();
    origUseMeasurement = config.isUSE_MEASUREMENT();
    origDeviceNumber = config.getDEVICE_NUMBER();
    origDataClientNumber = config.getDATA_CLIENT_NUMBER();
    origSchemaClientNumber = config.getSCHEMA_CLIENT_NUMBER();
    origLoop = config.getLOOP();
    origBatchSize = config.getBATCH_SIZE_PER_WRITE();
    origCreateSchema = config.isCREATE_SCHEMA();
    origDeleteData = config.isIS_DELETE_DATA();
    origClientBind = config.isIS_CLIENT_BIND();
    origOperationProportion = config.getOPERATION_PROPORTION();
    origResultPrintInterval = config.getRESULT_PRINT_INTERVAL();
    origTestMaxTime = config.getTEST_MAX_TIME();

    // Talk to FakeDB only — no real database, every operation returns success.
    config.getDbConfig().setDB_SWITCH(DBSwitch.DB_FAKE);
    // Do not write CSV/MySQL/IoTDB persistence files during the test.
    config.setTEST_DATA_PERSISTENCE(Constants.TDP_NONE);
    config.setUSE_MEASUREMENT(true);
    config.setBENCHMARK_WORK_MODE(BenchmarkMode.TEST_WITH_DEFAULT_PATH);
    config.setDEVICE_NUMBER(DEVICES);
    config.setDATA_CLIENT_NUMBER(1);
    config.setSCHEMA_CLIENT_NUMBER(1);
    config.setLOOP(100);
    config.setBATCH_SIZE_PER_WRITE(10);
    config.setCREATE_SCHEMA(true);
    // FakeDB has no data to delete; skip cleanup to keep the run minimal.
    config.setIS_DELETE_DATA(false);
    config.setIS_CLIENT_BIND(true);
    // write : preciseQuery : rangeQuery : (rest 0) — exercises both the write and read paths.
    config.setOPERATION_PROPORTION("1:1:1:0:0:0:0:0:0:0:0:0:0");
    // Disable the periodic background schedulers so run() stays self-contained and fast.
    config.setRESULT_PRINT_INTERVAL(0);
    config.setTEST_MAX_TIME(0L);
  }

  @After
  public void tearDown() {
    config.getDbConfig().setDB_SWITCH(origDbSwitch);
    config.setTEST_DATA_PERSISTENCE(origPersistence);
    config.setUSE_MEASUREMENT(origUseMeasurement);
    config.setDEVICE_NUMBER(origDeviceNumber);
    config.setDATA_CLIENT_NUMBER(origDataClientNumber);
    config.setSCHEMA_CLIENT_NUMBER(origSchemaClientNumber);
    config.setLOOP(origLoop);
    config.setBATCH_SIZE_PER_WRITE(origBatchSize);
    config.setCREATE_SCHEMA(origCreateSchema);
    config.setIS_DELETE_DATA(origDeleteData);
    config.setIS_CLIENT_BIND(origClientBind);
    config.setOPERATION_PROPORTION(origOperationProportion);
    config.setRESULT_PRINT_INTERVAL(origResultPrintInterval);
    config.setTEST_MAX_TIME(origTestMaxTime);
  }

  @Test
  public void fullRunAgainstFakeDbWritesAndReadsSomething() {
    TestWithDefaultPathMode mode = new TestWithDefaultPathMode();
    mode.run();

    Measurement measurement = mode.baseModeMeasurement;

    long ingestionOps = measurement.getOkOperationNum(Operation.INGESTION);
    long ingestionPoints = measurement.getOkPointNum(Operation.INGESTION);
    assertTrue(
        "the benchmark completed but recorded no successful writes — it ran against nothing",
        ingestionOps > 0);
    assertTrue("successful writes recorded no data points", ingestionPoints > 0);

    long readOps =
        measurement.getOkOperationNum(Operation.PRECISE_QUERY)
            + measurement.getOkOperationNum(Operation.RANGE_QUERY);
    assertTrue("the benchmark recorded no successful reads", readOps > 0);

    long totalFailures = 0;
    for (Operation operation : Operation.getNormalOperation()) {
      totalFailures += measurement.getFailOperationNum(operation);
    }
    assertEquals(
        "FakeDB returns success for every operation; nothing should fail", 0L, totalFailures);

    assertTrue("elapsed time was not recorded", measurement.getElapseTime() > 0);
  }
}
