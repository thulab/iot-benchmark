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
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Data Schema for generate data */
public class GenerateMetaDataSchema extends MetaDataSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateMetaDataSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Integer TYPE_NUMBER = 6;

  @Override
  public boolean createMetaDataSchema() {
    Map<String, SensorType> sensorTypes = getSensorTypes();
    if (sensorTypes == null) {
      return false;
    }
    List<String> sensors = sortSensors(sensorTypes.keySet());

    int eachClientDeviceNum = config.getDEVICE_NUMBER() / config.getCLIENT_NUMBER();
    // The part that cannot be divided equally is given to clients with a smaller number
    int leftClientDeviceNum = config.getDEVICE_NUMBER() % config.getCLIENT_NUMBER();
    int deviceId = MetaUtil.getDeviceId(0);
    for (int clientId = 0; clientId < config.getCLIENT_NUMBER(); clientId++) {
      int deviceNumber =
          (clientId < leftClientDeviceNum) ? eachClientDeviceNum + 1 : eachClientDeviceNum;
      List<DeviceSchema> deviceSchemaList = new ArrayList<>();
      for (int d = 0; d < deviceNumber; d++) {
        DeviceSchema deviceSchema = new DeviceSchema(deviceId, sensors);
        NAME_DATA_SCHEMA.put(deviceSchema.getDevice(), deviceSchema);
        addSensorType(deviceSchema.getDevice(), sensorTypes);
        deviceSchemaList.add(deviceSchema);
        deviceId++;
      }
      CLIENT_DATA_SCHEMA.put(clientId, deviceSchemaList);
    }
    return true;
  }

  private Map<String, SensorType> getSensorTypes() {
    double[] probabilities = generateProbabilities();
    if (probabilities == null) {
      return null;
    }
    Map<String, SensorType> sensors = new HashMap<>();
    for (int sensorIndex = 0; sensorIndex < config.getSENSOR_NUMBER(); sensorIndex++) {
      double sensorPosition = sensorIndex * 1.0 / config.getSENSOR_NUMBER();
      int i;
      for (i = 1; i <= TYPE_NUMBER; i++) {
        if (sensorPosition >= probabilities[i - 1] && sensorPosition < probabilities[i]) {
          break;
        }
      }
      String sensorName = MetaUtil.getSensorName(sensorIndex);
      sensors.put(sensorName, SensorType.getType(i));
    }
    return sensors;
  }

  /**
   * Generate Probabilities according to proportion(e.g. 1:1:1:1:1:1)
   *
   * @return
   */
  private double[] generateProbabilities() {
    // Probabilities for Types
    double[] probabilities = new double[TYPE_NUMBER + 1];
    // Origin proportion array
    double[] proportions = new double[TYPE_NUMBER];
    // unified proportion array
    List<Double> proportion = new ArrayList<>();
    LOGGER.info(
        "Init SensorTypes: BOOLEAN:INT32:INT64:FLOAT:DOUBLE:TEXT="
            + config.getINSERT_DATATYPE_PROPORTION());

    String[] split = config.getINSERT_DATATYPE_PROPORTION().split(":");
    if (split.length != TYPE_NUMBER) {
      LOGGER.error("INSERT_DATATYPE_PROPORTION error, please check this parameter.");
      return null;
    }
    double sum = 0;
    for (int i = 0; i < TYPE_NUMBER; i++) {
      proportions[i] = Double.parseDouble(split[i]);
      sum += proportions[i];
    }
    for (int i = 0; i < TYPE_NUMBER; i++) {
      if (sum != 0) {
        proportion.add(proportions[i] / sum);
      } else {
        proportion.add(0.0);
        LOGGER.error("The sum of INSERT_DATATYPE_PROPORTION is zero!");
      }
    }
    probabilities[0] = 0.0;
    for (int i = 1; i <= TYPE_NUMBER; i++) {
      probabilities[i] = probabilities[i - 1] + proportion.get(i - 1);
    }
    return probabilities;
  }
}
