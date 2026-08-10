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
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBSwitch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConfigDescriptorTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private BenchmarkMode originalMode;
  private boolean originalClientBind;
  private int originalDeviceNumber;
  private int originalSchemaClientNumber;
  private int originalDataClientNumber;
  private SQLDialect originalDialect;
  private DBSwitch originalDbSwitch;
  private boolean originalDoubleWrite;
  private DBSwitch originalAnotherDbSwitch;
  private int originalTableNumber;
  private int originalTagNumber;
  private List<Integer> originalTagValueCardinality;
  private boolean originalEnableQueryTagFilter;
  private int originalQueryTagIndex;
  private int originalQueryTagValueNum;

  @Before
  public void before() {
    originalMode = config.getBENCHMARK_WORK_MODE();
    originalClientBind = config.isIS_CLIENT_BIND();
    originalDeviceNumber = config.getDEVICE_NUMBER();
    originalSchemaClientNumber = config.getSCHEMA_CLIENT_NUMBER();
    originalDataClientNumber = config.getDATA_CLIENT_NUMBER();
    originalDialect = config.getIoTDB_DIALECT_MODE();
    originalDbSwitch = config.getDbConfig().getDB_SWITCH();
    originalDoubleWrite = config.isIS_DOUBLE_WRITE();
    originalAnotherDbSwitch = config.getANOTHER_DBConfig().getDB_SWITCH();
    originalTableNumber = config.getIoTDB_TABLE_NUMBER();
    originalTagNumber = config.getTAG_NUMBER();
    originalTagValueCardinality = new ArrayList<>(config.getTAG_VALUE_CARDINALITY());
    originalEnableQueryTagFilter = config.isENABLE_QUERY_TAG_FILTER();
    originalQueryTagIndex = config.getQUERY_TAG_INDEX();
    originalQueryTagValueNum = config.getQUERY_TAG_VALUE_NUM();
  }

  @After
  public void after() {
    // restore the shared config singleton so this test does not pollute others
    config.setBENCHMARK_WORK_MODE(originalMode);
    config.setIS_CLIENT_BIND(originalClientBind);
    config.setDEVICE_NUMBER(originalDeviceNumber);
    config.setSCHEMA_CLIENT_NUMBER(originalSchemaClientNumber);
    config.setDATA_CLIENT_NUMBER(originalDataClientNumber);
    config.setIoTDB_DIALECT_MODE(originalDialect);
    config.setDB_SWITCH(originalDbSwitch);
    config.setIS_DOUBLE_WRITE(originalDoubleWrite);
    config.setANOTHER_DB_SWITCH(originalAnotherDbSwitch);
    config.setIoTDB_TABLE_NUMBER(originalTableNumber);
    config.setTAG_NUMBER(originalTagNumber);
    config.setTAG_VALUE_CARDINALITY(originalTagValueCardinality);
    config.setENABLE_QUERY_TAG_FILTER(originalEnableQueryTagFilter);
    config.setQUERY_TAG_INDEX(originalQueryTagIndex);
    config.setQUERY_TAG_VALUE_NUM(originalQueryTagValueNum);
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
  public void testDisabledQueryTagFilterKeepsExistingBehavior() {
    config.setENABLE_QUERY_TAG_FILTER(false);
    config.setIoTDB_DIALECT_MODE(SQLDialect.TREE);
    config.setTAG_NUMBER(0);
    config.setTAG_VALUE_CARDINALITY(new ArrayList<>());
    config.setQUERY_TAG_INDEX(-1);
    config.setQUERY_TAG_VALUE_NUM(-1);

    assertTrue(
        "tag-filter-specific constraints must be ignored when the feature is disabled",
        ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testQueryTagFilterAcceptsValueCountBoundaries() {
    configureValidQueryTagFilter();

    config.setQUERY_TAG_VALUE_NUM(0);
    assertTrue(
        "zero must mean all actual values of the selected tag in each query table",
        ConfigDescriptor.getInstance().checkConfig());

    config.setQUERY_TAG_VALUE_NUM(2);
    assertTrue(
        "the selected tag cardinality itself must be accepted",
        ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testQueryTagFilterRejectsInvalidTagSelection() {
    configureValidQueryTagFilter();

    config.setQUERY_TAG_INDEX(-1);
    assertFalse(ConfigDescriptor.getInstance().checkConfig());

    config.setQUERY_TAG_INDEX(2);
    assertFalse(ConfigDescriptor.getInstance().checkConfig());

    config.setQUERY_TAG_INDEX(1);
    config.setQUERY_TAG_VALUE_NUM(-1);
    assertFalse(ConfigDescriptor.getInstance().checkConfig());

    config.setQUERY_TAG_VALUE_NUM(3);
    assertFalse(ConfigDescriptor.getInstance().checkConfig());

    config.setQUERY_TAG_VALUE_NUM(0);
    config.setTAG_VALUE_CARDINALITY(Arrays.asList(3, 0));
    assertFalse(ConfigDescriptor.getInstance().checkConfig());

    config.setTAG_NUMBER(0);
    config.setTAG_VALUE_CARDINALITY(new ArrayList<>());
    assertFalse(ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testQueryTagFilterRejectsUnsupportedDatabaseOrDialect() {
    configureValidQueryTagFilter();
    assertTrue(ConfigDescriptor.getInstance().checkConfig());

    config.setIoTDB_DIALECT_MODE(SQLDialect.TREE);
    assertFalse(ConfigDescriptor.getInstance().checkConfig());

    config.setIoTDB_DIALECT_MODE(SQLDialect.TABLE);
    config.setDB_SWITCH(DBSwitch.DB_IOT_130_SESSION_BY_TABLET);
    assertFalse(ConfigDescriptor.getInstance().checkConfig());
  }

  @Test
  public void testQueryTagFilterRequiresBothDoubleWriteDatabasesToSupportIt() {
    configureValidQueryTagFilter();
    config.setIS_DOUBLE_WRITE(true);
    config.setANOTHER_DB_SWITCH(DBSwitch.DB_IOT_200_SESSION_BY_TABLET);
    assertTrue(ConfigDescriptor.getInstance().checkConfig());

    config.setANOTHER_DB_SWITCH(DBSwitch.DB_IOT_130_SESSION_BY_TABLET);
    assertFalse(ConfigDescriptor.getInstance().checkConfig());
  }

  private void configureValidQueryTagFilter() {
    config.setENABLE_QUERY_TAG_FILTER(true);
    config.setIoTDB_DIALECT_MODE(SQLDialect.TABLE);
    config.setDB_SWITCH(DBSwitch.DB_IOT_200_SESSION_BY_TABLET);
    config.setIS_DOUBLE_WRITE(false);
    config.setTAG_NUMBER(2);
    config.setTAG_VALUE_CARDINALITY(Arrays.asList(3, 2));
    config.setQUERY_TAG_INDEX(1);
    config.setQUERY_TAG_VALUE_NUM(0);
  }
}
