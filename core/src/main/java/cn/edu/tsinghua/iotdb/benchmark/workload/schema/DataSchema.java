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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** Data Schema for generate data */
public class DataSchema extends BaseDataSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  protected void createDataSchema() {
    int eachClientDeviceNum;
    if (config.getCLIENT_NUMBER() != 0) {
      eachClientDeviceNum = config.getDEVICE_NUMBER() / config.getCLIENT_NUMBER();
    } else {
      LOGGER.error("getCLIENT_NUMBER() can not be zero.");
      return;
    }

    int deviceId = 0;
    // The number of devices that cannot be divided equally
    int mod = config.getDEVICE_NUMBER() % config.getCLIENT_NUMBER();
    for (int clientId = 0; clientId < config.getCLIENT_NUMBER(); clientId++) {
      List<DeviceSchema> deviceSchemaList = new ArrayList<>();
      for (int j = 0; j < eachClientDeviceNum; j++) {
        deviceSchemaList.add(new DeviceSchema(config.getDEVICE_CODES().get(deviceId++)));
      }
      // The part that cannot be divided equally is given to clients with a smaller number.
      if (clientId < mod) {
        deviceSchemaList.add(new DeviceSchema(config.getDEVICE_CODES().get(deviceId++)));
      }
      CLIENT_BIND_SCHEMA.put(clientId, deviceSchemaList);
    }
  }
}
