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
    // schemaClient
    Map<Integer, Integer> deviceDistributionForSchemaClient =
        CommonAlgorithms.distributeDevicesToClients(
            config.getDEVICE_NUMBER(), config.getSCHEMA_CLIENT_NUMBER());
    int deviceIndexForSchema = MetaUtil.getDeviceId(0);
    for (int clientId = 0; clientId < config.getSCHEMA_CLIENT_NUMBER(); clientId++) {
      int deviceNumber = deviceDistributionForSchemaClient.get(clientId);
      List<DeviceSchema> deviceSchemasList = new ArrayList<>();
      for (int d = 0; d < deviceNumber; d++) {
        DeviceSchema deviceSchema =
            new DeviceSchema(deviceIndexForSchema, sensors, MetaUtil.getTags(deviceIndexForSchema));
        deviceSchemasList.add(deviceSchema);
        deviceIndexForSchema++;
      }
      SCHEMA_CLIENT_DATA_SCHEMA.put(clientId, deviceSchemasList);
    }

    // dataClient
    Map<Integer, Integer> deviceDistributionForDataClient =
        CommonAlgorithms.distributeDevicesToClients(
            config.getDEVICE_NUMBER(), config.getDATA_CLIENT_NUMBER());
    int deviceIndex = MetaUtil.getDeviceId(0);
    // Rearrange device IDs so that adjacent devices are in the same table
    List<Integer> deviceIds = MetaUtil.sortDeviceId(config, LOGGER);
    for (int clientId = 0; clientId < config.getDATA_CLIENT_NUMBER(); clientId++) {
      int deviceNumber = deviceDistributionForDataClient.get(clientId);
      List<DeviceSchema> deviceSchemaList = new ArrayList<>();
      for (int d = 0; d < deviceNumber; d++) {
        DeviceSchema deviceSchema;
        deviceSchema =
            new DeviceSchema(
                deviceIds.get(deviceIndex), sensors, MetaUtil.getTags(deviceIds.get(deviceIndex)));
        NAME_DATA_SCHEMA.put(deviceSchema.getDevice(), deviceSchema);
        GROUPS.add(deviceSchema.getGroup());
        deviceSchemaList.add(deviceSchema);
        deviceIndex++;
      }
      DATA_CLIENT_DATA_SCHEMA.put(clientId, deviceSchemaList);
    }
    return true;
  }
}
