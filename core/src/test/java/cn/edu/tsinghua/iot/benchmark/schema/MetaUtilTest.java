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

package cn.edu.tsinghua.iot.benchmark.schema;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetaUtilTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private String originalStrategy;

  @Before
  public void before() {
    originalStrategy = config.getSG_STRATEGY();
    config.setSG_STRATEGY(Constants.MOD_SG_ASSIGN_MODE);
  }

  @After
  public void after() {
    config.setSG_STRATEGY(originalStrategy);
  }

  @Test
  public void testMappingIdModInRangeAndValue() throws WorkloadException {
    int alloc = 8;
    for (int objectId = 0; objectId < 1000; objectId++) {
      int mapped = MetaUtil.mappingId(objectId, 1000, alloc);
      assertTrue("mappingId must be in [0, alloc)", mapped >= 0 && mapped < alloc);
      assertEquals(objectId % alloc, mapped);
    }
  }

  @Test(expected = WorkloadException.class)
  public void testMappingIdUnsupportedStrategyThrows() throws WorkloadException {
    config.setSG_STRATEGY("NOT_A_REAL_STRATEGY");
    MetaUtil.mappingId(1, 10, 2);
  }

  @Test
  public void testNameFormatting() {
    assertEquals(config.getDEVICE_NAME_PREFIX() + "5", MetaUtil.getDeviceName(5));
    assertEquals(config.getGROUP_NAME_PREFIX() + "3", MetaUtil.getGroupName(3));
    assertEquals(config.getSENSOR_NAME_PREFIX() + "7", MetaUtil.getSensorName(7));
    assertEquals(config.getIoTDB_TABLE_NAME_PREFIX() + "2", MetaUtil.getTableName(2));
  }

  @Test
  public void testDeviceIdFromStrNonNegative() {
    for (String s : new String[] {"d_0", "device_123", "x", "时序"}) {
      assertTrue("hashCode-derived id must be non-negative", MetaUtil.getDeviceIdFromStr(s) >= 0);
    }
  }

  @Test
  public void testGroupIdFromDeviceNameInRange() {
    int groupNumber = config.getGROUP_NUMBER();
    for (int i = 0; i < 100; i++) {
      int groupId = Integer.parseInt(MetaUtil.getGroupIdFromDeviceName(MetaUtil.getDeviceName(i)));
      assertTrue("groupId must be in [0, groupNumber)", groupId >= 0 && groupId < groupNumber);
    }
  }
}
