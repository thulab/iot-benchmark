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

package cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** Data Schema for generate data */
public class GenerateMetaDataSchema extends MetaDataSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateMetaDataSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  public boolean createMetaDataSchema() {
    List<Sensor> sensors = config.getSENSORS();
    if (sensors == null) {
      return false;
    }

    int eachClientDeviceNum = config.getDEVICE_NUMBER() / config.getCLIENT_NUMBER();
    // The part that cannot be divided equally is given to clients with a smaller number
    int leftClientDeviceNum = config.getDEVICE_NUMBER() % config.getCLIENT_NUMBER();
    int deviceId = MetaUtil.getDeviceId(0);
    for (int clientId = 0; clientId < config.getCLIENT_NUMBER(); clientId++) {
      int deviceNumber =
          (clientId < leftClientDeviceNum) ? eachClientDeviceNum + 1 : eachClientDeviceNum;
      List<DeviceSchema> deviceSchemaList = new ArrayList<>();
      for (int d = 0; d < deviceNumber; d++) {
        // TODO generate tags
        DeviceSchema deviceSchema = new DeviceSchema(deviceId, sensors, new HashMap<>());
        NAME_DATA_SCHEMA.put(deviceSchema.getDevice(), deviceSchema);
        deviceSchemaList.add(deviceSchema);
        deviceId++;
      }
      CLIENT_DATA_SCHEMA.put(clientId, deviceSchemaList);
    }
    return true;
  }
}
