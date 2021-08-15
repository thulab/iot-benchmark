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
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.utils.MetaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Data Schema for generate data */
public class GenerateDataSchema extends BaseDataSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Integer TYPE_NUMBER = 6;

  @Override
  protected void createDataSchema() {
    Map<String, Type> sensorTypes = getSensorTypes();
    List<String> sensors = new ArrayList<>(sensorTypes.keySet());
    sensors.sort(
        new Comparator<String>() {
          @Override
          public int compare(String o1, String o2) {
            return Integer.valueOf(o1.replace(Constants.SENSOR_NAME_PREFIX, ""))
                - Integer.valueOf(o2.replace(Constants.SENSOR_NAME_PREFIX, ""));
          }
        });
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
        deviceSchemaList.add(new DeviceSchema(config.getDEVICE_CODES().get(deviceId++), sensors));
        addSensorType(MetaUtil.getDeviceName(MetaUtil.getDeviceId(deviceId - 1)), sensorTypes);
      }
      // The part that cannot be divided equally is given to clients with a smaller number.
      if (clientId < mod) {
        deviceSchemaList.add(new DeviceSchema(config.getDEVICE_CODES().get(deviceId++), sensors));
        addSensorType(MetaUtil.getDeviceName(MetaUtil.getDeviceId(deviceId - 1)), sensorTypes);
      }
      CLIENT_BIND_SCHEMA.put(clientId, deviceSchemaList);
    }
  }

  private Map<String, Type> getSensorTypes() {
    double[] probabilities = generateProbabilities();
    Map<String, Type> sensors = new HashMap<>();
    for (int sensorIndex = 0; sensorIndex < config.getSENSOR_NUMBER(); sensorIndex++) {
      double sensorPosition = sensorIndex * 1.0 / config.getSENSOR_NUMBER();
      int i;
      for (i = 1; i <= 6; i++) {
        if (sensorPosition >= probabilities[i - 1] && sensorPosition < probabilities[i]) {
          break;
        }
      }
      String sensorName = MetaUtil.getSensorName(sensorIndex);
      sensors.put(sensorName, Type.getType(i));
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
    LOGGER.info("Init SensorTypes!");

    String[] split = config.getINSERT_DATATYPE_PROPORTION().split(":");
    if (split.length != TYPE_NUMBER) {
      LOGGER.error("INSERT_DATATYPE_PROPORTION error, please check this parameter.");
      System.exit(1);
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
