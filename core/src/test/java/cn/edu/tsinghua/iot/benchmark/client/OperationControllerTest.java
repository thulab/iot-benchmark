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

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.client.operation.OperationController;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OperationControllerTest {

  private static Config config = ConfigDescriptor.getInstance().getConfig();

  @Before
  public void before() {}

  @After
  public void after() {}

  @Test
  public void testGetNextOperationType() {
    config.setOPERATION_PROPORTION("1:0:0:0:0:0:0:0:0:0:0");
    OperationController operationController = new OperationController(0);

    int loop = 10000;
    for (int i = 0; i < loop; i++) {
      Assert.assertEquals(Operation.INGESTION, operationController.getNextOperationType());
    }

    config.setOPERATION_PROPORTION("0:1:0:0:0:0:0:0:0:0:0");
    operationController = new OperationController(0);
    for (int i = 0; i < loop; i++) {
      assertEquals(Operation.PRECISE_QUERY, operationController.getNextOperationType());
    }
  }
}
