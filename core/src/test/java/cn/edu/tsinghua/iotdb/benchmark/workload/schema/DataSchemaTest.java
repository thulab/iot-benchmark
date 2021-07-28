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

package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DataSchemaTest {
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  @Test
  public void test() {
    testBalanceSplit();
  }

  void testBalanceSplit() {
    int preDeviceNum = config.getDEVICE_NUMBER();
    int preClientNum = config.getCLIENT_NUMBER();
    config.setDEVICE_NUMBER(100);
    config.setCLIENT_NUMBER(30);
    int mod = config.getDEVICE_NUMBER() % config.getCLIENT_NUMBER();
    int deviceNumEachClient = config.getDEVICE_NUMBER() / config.getCLIENT_NUMBER();
    config.initDeviceCodes();
    DataSchema dataSchema = DataSchema.getInstance();
    Map<Integer, List<DeviceSchema>> client2Schema = dataSchema.getClientBindSchema();
    for (int clientId : client2Schema.keySet()) {
      int deviceNumInClient = client2Schema.get(clientId).size();
      if (clientId < mod) {
        Assert.assertEquals(deviceNumEachClient + 1, deviceNumInClient);
      } else {
        Assert.assertEquals(deviceNumEachClient, deviceNumInClient);
      }
    }
    config.setDEVICE_NUMBER(preDeviceNum);
    config.setCLIENT_NUMBER(preClientNum);
  }
}
