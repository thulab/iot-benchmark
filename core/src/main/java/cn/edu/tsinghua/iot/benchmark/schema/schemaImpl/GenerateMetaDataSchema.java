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

package cn.edu.tsinghua.iot.benchmark.schema.schemaImpl;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.utils.CommonAlgorithms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    int deviceId = MetaUtil.getDeviceId(0);
    Map<Integer, Integer> deviceDistribution =
        CommonAlgorithms.distributeDevicesToClients(
            config.getDEVICE_NUMBER(), config.getCLIENT_NUMBER());
    for (int clientId = 0; clientId < config.getCLIENT_NUMBER(); clientId++) {
      int deviceNumber = deviceDistribution.get(clientId);
      List<DeviceSchema> deviceSchemaList = new ArrayList<>();
      for (int d = 0; d < deviceNumber; d++) {
        DeviceSchema deviceSchema = new DeviceSchema(deviceId, sensors, MetaUtil.getTags(deviceId));
        NAME_DATA_SCHEMA.put(deviceSchema.getDevice(), deviceSchema);
        GROUPS.add(deviceSchema.getGroup());
        deviceSchemaList.add(deviceSchema);
        deviceId++;
      }
      CLIENT_DATA_SCHEMA.put(clientId, deviceSchemaList);
    }
    return true;
  }
}
