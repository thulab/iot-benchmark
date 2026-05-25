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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConfigDescriptorTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private BenchmarkMode originalMode;
  private boolean originalClientBind;
  private int originalDeviceNumber;
  private int originalSchemaClientNumber;
  private int originalDataClientNumber;

  @Before
  public void before() {
    originalMode = config.getBENCHMARK_WORK_MODE();
    originalClientBind = config.isIS_CLIENT_BIND();
    originalDeviceNumber = config.getDEVICE_NUMBER();
    originalSchemaClientNumber = config.getSCHEMA_CLIENT_NUMBER();
    originalDataClientNumber = config.getDATA_CLIENT_NUMBER();
  }

  @After
  public void after() {
    // restore the shared config singleton so this test does not pollute others
    config.setBENCHMARK_WORK_MODE(originalMode);
    config.setIS_CLIENT_BIND(originalClientBind);
    config.setDEVICE_NUMBER(originalDeviceNumber);
    config.setSCHEMA_CLIENT_NUMBER(originalSchemaClientNumber);
    config.setDATA_CLIENT_NUMBER(originalDataClientNumber);
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
}
