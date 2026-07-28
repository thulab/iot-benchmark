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

package cn.edu.tsinghua.iot.benchmark.client;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.client.operation.OperationController;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OperationControllerTest extends BenchmarkTestBase {

  private static Config config = ConfigDescriptor.getInstance().getConfig();

  private String originalProportion;

  @Before
  public void before() {
    originalProportion = config.getOPERATION_PROPORTION();
  }

  @After
  public void after() {
    // restore the shared config singleton so this test does not pollute others
    config.setOPERATION_PROPORTION(originalProportion);
  }

  @Test
  public void testGetNextOperationType() {
    // length is derived from the operation set so adding new operations does not break this test
    int opCount = Operation.getNormalOperation().size();
    int loop = 10000;

    config.setOPERATION_PROPORTION(proportionWithSingleOne(opCount, 0));
    OperationController operationController = new OperationController(0);
    for (int i = 0; i < loop; i++) {
      Assert.assertEquals(Operation.INGESTION, operationController.getNextOperationType());
    }

    config.setOPERATION_PROPORTION(proportionWithSingleOne(opCount, 1));
    operationController = new OperationController(0);
    for (int i = 0; i < loop; i++) {
      assertEquals(Operation.PRECISE_QUERY, operationController.getNextOperationType());
    }
  }

  /**
   * Build an OPERATION_PROPORTION string of {@code length} colon-separated fields, with "1" at
   * {@code oneIndex} and "0" everywhere else.
   */
  private static String proportionWithSingleOne(int length, int oneIndex) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i > 0) {
        sb.append(":");
      }
      sb.append(i == oneIndex ? "1" : "0");
    }
    return sb.toString();
  }
}
