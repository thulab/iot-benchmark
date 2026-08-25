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

package cn.edu.tsinghua.iot.benchmark.conf;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBSwitch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConfigDescriptorTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private BenchmarkMode originalMode;
  private boolean originalClientBind;
  private int originalDeviceNumber;
  private int originalSchemaClientNumber;
  private int originalDataClientNumber;
  private int originalIoTDBThriftMaxFrameSize;
  private double originalNullRatio;
  private boolean originalDoubleWrite;
  private DBSwitch originalDbSwitch;
  private DBSwitch originalAnotherDbSwitch;

  @Before
  public void before() {
    originalMode = config.getBENCHMARK_WORK_MODE();
    originalClientBind = config.isIS_CLIENT_BIND();
    originalDeviceNumber = config.getDEVICE_NUMBER();
    originalSchemaClientNumber = config.getSCHEMA_CLIENT_NUMBER();
    originalDataClientNumber = config.getDATA_CLIENT_NUMBER();
    originalIoTDBThriftMaxFrameSize = config.getIOTDB_THRIFT_MAX_FRAME_SIZE();
    originalNullRatio = config.getNULL_RATIO();
    originalDoubleWrite = config.isIS_DOUBLE_WRITE();
    originalDbSwitch = config.getDbConfig().getDB_SWITCH();
    originalAnotherDbSwitch = config.getANOTHER_DBConfig().getDB_SWITCH();
  }

  @After
  public void after() {
    // restore the shared config singleton so this test does not pollute others
    config.setBENCHMARK_WORK_MODE(originalMode);
    config.setIS_CLIENT_BIND(originalClientBind);
    config.setDEVICE_NUMBER(originalDeviceNumber);
    config.setSCHEMA_CLIENT_NUMBER(originalSchemaClientNumber);
    config.setDATA_CLIENT_NUMBER(originalDataClientNumber);
    config.setIOTDB_THRIFT_MAX_FRAME_SIZE(originalIoTDBThriftMaxFrameSize);
    config.setNULL_RATIO(originalNullRatio);
    config.setIS_DOUBLE_WRITE(originalDoubleWrite);
    config.getDbConfig().setDB_SWITCH(originalDbSwitch);
    config.getANOTHER_DBConfig().setDB_SWITCH(originalAnotherDbSwitch);
  }

  /**
   * Regression test for issue #10: under {@code IS_CLIENT_BIND}, the validation joined the two
   * "device number &lt; client number" checks with {@code &&}, so a config that has fewer devices
   * than data clients (but not fewer than schema clients) was wrongly accepted. The fix uses {@code
   * ||}.
   *
   * <p>The baseline assertion (enough devices -&gt; valid) guarantees the rest of {@code
   * checkConfig()} passes for this config, so the {@code false} result below can only come from the
   * client-bind rule.
   */
  @Test
  public void testClientBindRejectsMoreClientsThanDevices() {
    config.setBENCHMARK_WORK_MODE(BenchmarkMode.TEST_WITH_DEFAULT_PATH);
    config.setIS_CLIENT_BIND(true);
    config.setSCHEMA_CLIENT_NUMBER(1);
    config.setDATA_CLIENT_NUMBER(100);

    // baseline: device number >= every client number -> the rule must NOT trip
    config.setDEVICE_NUMBER(200);
    assertTrue(
        "config should be valid when device number >= every client number",
        ConfigDescriptor.getInstance().checkConfig());

    // device number < data client number under client-bind must be rejected.
    // Before the fix it was accepted because device (50) >= schema client (1).
    config.setDEVICE_NUMBER(50);
    assertFalse(
        "client-bind with device number < data client number must be rejected",
        ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testIoTDBThriftMaxFrameSize() {
    assertEquals(64 * 1024 * 1024, config.getIOTDB_THRIFT_MAX_FRAME_SIZE());

    config.setIOTDB_THRIFT_MAX_FRAME_SIZE(0);
    assertFalse(
        "IoTDB Thrift max frame size must be positive",
        ConfigDescriptor.getInstance().checkConfig());
  }

  /**
   * Sets up a config that is valid in every respect except NULL_RATIO, so a {@code false} result
   * from {@code checkConfig()} can only come from the NULL_RATIO guards.
   */
  private void setUpValidNullRatioContext() {
    config.setBENCHMARK_WORK_MODE(BenchmarkMode.TEST_WITH_DEFAULT_PATH);
    config.setIS_CLIENT_BIND(false);
    config.setIS_DOUBLE_WRITE(false);
    config.setDEVICE_NUMBER(6000);
    config.setSCHEMA_CLIENT_NUMBER(1);
    config.setDATA_CLIENT_NUMBER(1);
    config.setNULL_RATIO(0.9);
    config.getDbConfig().setDB_SWITCH(DBSwitch.DB_IOT_200_SESSION_BY_TABLET);
  }

  @Test
  public void testNullRatioSupportedCombinationIsAccepted() {
    setUpValidNullRatioContext();
    assertTrue(
        "NULL_RATIO with IoTDB-200-SESSION_BY_TABLET under testWithDefaultPath must be accepted",
        ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testNullRatioOutOfRangeRejected() {
    setUpValidNullRatioContext();
    config.setNULL_RATIO(1.5);
    assertFalse(
        "NULL_RATIO above 1 must be rejected", ConfigDescriptor.getInstance().checkConfig());
    config.setNULL_RATIO(-0.1);
    assertFalse(
        "NULL_RATIO below 0 must be rejected", ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testNullRatioRejectedInVerificationModes() {
    setUpValidNullRatioContext();
    config.setBENCHMARK_WORK_MODE(BenchmarkMode.VERIFICATION_WRITE);
    assertFalse(
        "NULL_RATIO must be rejected in verificationWriteMode",
        ConfigDescriptor.getInstance().checkConfig());
    config.setBENCHMARK_WORK_MODE(BenchmarkMode.VERIFICATION_QUERY);
    assertFalse(
        "NULL_RATIO must be rejected in verificationQueryMode",
        ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testNullRatioRejectedForOtherDatabases() {
    setUpValidNullRatioContext();
    config.getDbConfig().setDB_SWITCH(DBSwitch.DB_INFLUX);
    assertFalse(
        "NULL_RATIO must be rejected for non-IoTDB databases",
        ConfigDescriptor.getInstance().checkConfig());
    // iotdb-1.1 is not supported either
    config.getDbConfig().setDB_SWITCH(DBSwitch.DB_IOT_110_SESSION_BY_TABLET);
    assertFalse(
        "NULL_RATIO must be rejected for iotdb-1.1", ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testNullRatioAcceptedForIotdb13() {
    setUpValidNullRatioContext();
    config.getDbConfig().setDB_SWITCH(DBSwitch.DB_IOT_130_SESSION_BY_TABLET);
    assertTrue(
        "NULL_RATIO with IoTDB-130-SESSION_BY_TABLET must be accepted",
        ConfigDescriptor.getInstance().checkConfig());
    config.getDbConfig().setDB_SWITCH(DBSwitch.DB_IOT_130_JDBC);
    assertTrue(
        "NULL_RATIO with IoTDB-130-JDBC must be accepted",
        ConfigDescriptor.getInstance().checkConfig());
    config.getDbConfig().setDB_SWITCH(DBSwitch.DB_IOT_130_SESSION_BY_RECORD);
    assertFalse(
        "NULL_RATIO must be rejected for iotdb-1.3 SESSION_BY_RECORD",
        ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testNullRatioRejectedForUnsupportedInsertModes() {
    setUpValidNullRatioContext();
    config.getDbConfig().setDB_SWITCH(DBSwitch.DB_IOT_200_SESSION_BY_RECORD);
    assertFalse(
        "NULL_RATIO must be rejected for SESSION_BY_RECORD",
        ConfigDescriptor.getInstance().checkConfig());
    config.getDbConfig().setDB_SWITCH(DBSwitch.DB_IOT_200_REST);
    assertFalse(
        "NULL_RATIO must be rejected for the REST insert mode",
        ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testNullRatioChecksAnotherDbSwitchUnderDoubleWrite() {
    setUpValidNullRatioContext();
    config.setIS_DOUBLE_WRITE(true);
    config.getDbConfig().setDB_SWITCH(DBSwitch.DB_IOT_200_SESSION_BY_TABLET);
    config.getANOTHER_DBConfig().setDB_SWITCH(DBSwitch.DB_IOT_200_SESSION_BY_TABLET);
    assertTrue(
        "NULL_RATIO with both sides IoTDB-2.0 under double write must be accepted",
        ConfigDescriptor.getInstance().checkConfig());

    // iotdb-1.3 is also supported
    config.getANOTHER_DBConfig().setDB_SWITCH(DBSwitch.DB_IOT_130_SESSION_BY_TABLET);
    assertTrue(
        "NULL_RATIO with another side IoTDB-1.3 under double write must be accepted",
        ConfigDescriptor.getInstance().checkConfig());

    // iotdb-1.1 is not supported
    config.getANOTHER_DBConfig().setDB_SWITCH(DBSwitch.DB_IOT_110_SESSION_BY_TABLET);
    assertFalse(
        "NULL_RATIO must be rejected when ANOTHER_DB_SWITCH is not IoTDB-1.3/2.0",
        ConfigDescriptor.getInstance().checkConfig());
  }
}
