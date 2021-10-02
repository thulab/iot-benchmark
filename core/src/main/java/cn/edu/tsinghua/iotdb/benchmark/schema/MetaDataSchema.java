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

package cn.edu.tsinghua.iotdb.benchmark.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.schema.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.GenerateMetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.RealMetaDataSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** meta data schema */
public abstract class MetaDataSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataSchema.class);
  private static final String UNKNOWN_DEVICE = "Unknown device: %s";
  private static final String UNKNOWN_SENSOR = "Unknown type: device: %s, sensor: %s";

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  /** DeviceSchema for each client */
  protected static final Map<Integer, List<DeviceSchema>> CLIENT_DATA_SCHEMA =
      new ConcurrentHashMap<>();
  /** Name mapping of DeviceSchema */
  protected static final Map<String, DeviceSchema> NAME_DATA_SCHEMA = new ConcurrentHashMap<>();
  /**
   * Type mapping for each sensor, rule: Device Name (e.g. d_0) -> Sensor Name (e.g. s_0) -> Sensor
   * Type
   */
  protected static final Map<String, Map<String, SensorType>> TYPE_MAPPING =
      new ConcurrentHashMap<>();
  /** The singleton of BaseDataSchema */
  private static MetaDataSchema metaDataSchema = null;
  /** The init method of MetaDataSchema */
  protected MetaDataSchema() {
    if (!createMetaDataSchema()) {
      System.exit(1);
    }
  }

  /**
   * init data schema for each device
   *
   * @return
   */
  protected abstract boolean createMetaDataSchema();

  protected List<String> sortSensors(Set<String> sensors) {
    List<String> result = new ArrayList<>(sensors);
    result.sort(
        Comparator.comparingInt(o -> Integer.valueOf(o.replace(Constants.SENSOR_NAME_PREFIX, ""))));
    return result;
  }

  /**
   * Add sensor types into TYPE_MAPPING
   *
   * @param deviceName
   * @param types
   */
  public void addSensorType(String deviceName, Map<String, SensorType> types) {
    TYPE_MAPPING.put(deviceName, types);
  }

  /**
   * Get Sensor type
   *
   * @param deviceName e.g. d_0
   * @param sensorName e.g. s_0
   * @return
   */
  public SensorType getSensorType(String deviceName, String sensorName) {
    try {
      return TYPE_MAPPING.get(deviceName).get(sensorName);
    } catch (Exception e) {
      LOGGER.warn(String.format(UNKNOWN_SENSOR, deviceName, sensorName));
      return SensorType.TEXT;
    }
  }

  /**
   * Get DeviceSchema by device
   *
   * @param deviceName
   * @return
   */
  public DeviceSchema getDeviceSchemaByName(String deviceName) {
    try {
      return NAME_DATA_SCHEMA.get(deviceName);
    } catch (Exception e) {
      LOGGER.warn(String.format(UNKNOWN_DEVICE, deviceName));
      return null;
    }
  }

  /**
   * Get DeviceSchema by clientId
   *
   * @param clientId
   * @return
   */
  public List<DeviceSchema> getDeviceSchemaByClientId(int clientId) {
    return CLIENT_DATA_SCHEMA.get(clientId);
  }

  /**
   * Get All Device Schema
   *
   * @return
   */
  public List<DeviceSchema> getAllDeviceSchemas() {
    return new ArrayList<>(NAME_DATA_SCHEMA.values());
  }

  /** Singleton */
  public static MetaDataSchema getInstance() {
    if (metaDataSchema == null) {
      synchronized (MetaDataSchema.class) {
        if (metaDataSchema == null) {
          // TODO init metadata schema
          switch (config.getBENCHMARK_WORK_MODE()) {
            case VERIFICATION_WRITE:
            case VERIFICATION_QUERY:
              metaDataSchema = new RealMetaDataSchema();
              break;
            default:
              metaDataSchema = new GenerateMetaDataSchema();
              break;
          }
        }
      }
    }
    return metaDataSchema;
  }
}
