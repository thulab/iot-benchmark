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

package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class OperationControllerTest {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static OperationController operationController = new OperationController(0);

  @Before
  public void before() {}

  @After
  public void after() {}

  @Test
  public void testResolveOperationProportion() {
    config.setOPERATION_PROPORTION("1:1:0:1:0:1:0:1:0:0:0");
    Double[] expectedProbability = {0.2, 0.2, 0.0, 0.2, 0.0, 0.2, 0.0, 0.2, 0.0, 0.0, 0.0};
    List<Double> proportion = operationController.resolveOperationProportion();
    for (int i = 0; i < proportion.size(); i++) {
      assertEquals(expectedProbability[i], proportion.get(i));
    }
  }

  @Test
  public void testGetNextOperationType() {
    config.setOPERATION_PROPORTION("1:0:0:0:0:0:0:0:0:0:0");
    int loop = 10000;
    for (int i = 0; i < loop; i++) {
      assertEquals(Operation.INGESTION, operationController.getNextOperationType());
    }
    config.setOPERATION_PROPORTION("0:1:0:0:0:0:0:0:0:0:0");
    for (int i = 0; i < loop; i++) {
      assertEquals(Operation.PRECISE_QUERY, operationController.getNextOperationType());
    }
  }
}
