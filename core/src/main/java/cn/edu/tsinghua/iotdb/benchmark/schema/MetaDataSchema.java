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
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.GenerateMetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.RealMetaDataSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** meta data schema */
public abstract class MetaDataSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataSchema.class);
  private static final String UNKNOWN_DEVICE = "Unknown device: %s";

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  /** DeviceSchema for each client */
  protected static final Map<Integer, List<DeviceSchema>> CLIENT_DATA_SCHEMA =
      new ConcurrentHashMap<>();
  /** Name mapping of DeviceSchema */
  protected static final Map<String, DeviceSchema> NAME_DATA_SCHEMA = new ConcurrentHashMap<>();
  /** The set of group */
  protected static final Set<String> GROUPS = new HashSet<>();
  /** The singleton of BaseDataSchema */
  private static MetaDataSchema metaDataSchema = null;
  /** The init method of MetaDataSchema */
  protected MetaDataSchema() {
    if (!createMetaDataSchema()) {
      System.exit(1);
    }
  }

  /** init data schema for each device */
  protected abstract boolean createMetaDataSchema();

  protected List<Sensor> sortSensors(List<Sensor> sensors) {
    sensors.sort(
        Comparator.comparingInt(
            o -> Integer.parseInt(o.getName().replace(config.getSENSOR_NAME_PREFIX(), ""))));
    return sensors;
  }

  /** Get DeviceSchema by device */
  public DeviceSchema getDeviceSchemaByName(String deviceName) {
    try {
      return NAME_DATA_SCHEMA.get(deviceName);
    } catch (Exception e) {
      LOGGER.warn(String.format(UNKNOWN_DEVICE, deviceName));
      return null;
    }
  }

  /** Get DeviceSchema by clientId */
  public List<DeviceSchema> getDeviceSchemaByClientId(int clientId) {
    return CLIENT_DATA_SCHEMA.get(clientId);
  }

  /** Get All Device Schema */
  public List<DeviceSchema> getAllDeviceSchemas() {
    return new ArrayList<>(NAME_DATA_SCHEMA.values());
  }

  /** Get All Group */
  public Set<String> getAllGroups() {
    return GROUPS;
  }

  /** Singleton */
  public static MetaDataSchema getInstance() {
    if (metaDataSchema == null) {
      synchronized (MetaDataSchema.class) {
        if (metaDataSchema == null) {
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
