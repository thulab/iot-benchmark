/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iot.benchmark.schema.*;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.xml.DeviceXml;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.xml.IntervalXml;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.xml.SchemaXml;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.xml.SensorXml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class XmlMetaDataSchema extends MetaDataSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(XmlMetaDataSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final FunctionManager functionManager = FunctionManager.getInstance();
  private static final AtomicInteger deviceId = new AtomicInteger(0);

  @Override
  protected boolean createMetaDataSchema() {
    SchemaXml xml = null;
    String configFolder = System.getProperty(Constants.BENCHMARK_CONF, "configuration/conf");
    try {
      InputStream input = Files.newInputStream(Paths.get(configFolder + "/schema.xml"));
      JAXBContext context = JAXBContext.newInstance(SchemaXml.class);
      Unmarshaller unmarshaller = context.createUnmarshaller();
      xml = (SchemaXml) unmarshaller.unmarshal(input);
    } catch (Exception e) {
      LOGGER.error("Failed to load function xml", e);
      System.exit(0);
    }
    // TODO make interval valid
    List<IntervalXml> intervals = xml.getIntervals();
    Map<String, IntervalXml> intervalMap = new HashMap<>();
    for (IntervalXml intervalXml : intervals) {
      intervalMap.put(intervalXml.getId(), intervalXml);
    }
    List<DeviceSchema> deviceSchemas = new ArrayList<>();
    for (DeviceXml deviceXml : xml.getDevices()) {
      String deviceName = deviceXml.getName();
      List<Sensor> sensors = new ArrayList<>();
      for (SensorXml sensorXml : deviceXml.getSensors()) {
        FunctionParam functionParam = functionManager.getById(sensorXml.getBindFunction());
        if (functionParam == null) {
          LOGGER.error("Function id {} not found", sensorXml.getBindFunction());
          continue;
        }
        Sensor sensor =
            new Sensor(sensorXml.getName(), SensorType.getType(sensorXml.getType()), functionParam);
        sensors.add(sensor);
      }
      // TODO spricoder need to update device Id
      DeviceSchema deviceSchema =
          new DeviceSchema(
              deviceId.incrementAndGet(),
              MetaUtil.getGroupIdFromDeviceName(deviceName),
              deviceName,
              sensors,
              MetaUtil.getTags(deviceName));
      NAME_DATA_SCHEMA.put(deviceName, deviceSchema);
      GROUPS.add(deviceSchema.getGroup());
      deviceSchemas.add(deviceSchema);
    }
    if (deviceSchemas.size() < config.getCLIENT_NUMBER()) {
      LOGGER.error(
          "Device number {} is less than client number {}",
          deviceSchemas.size(),
          config.getCLIENT_NUMBER());
      return false;
    }
    // Split into client And store Type
    for (int i = 0; i < deviceSchemas.size(); i++) {
      int clientId = i % config.getCLIENT_NUMBER();
      DeviceSchema deviceSchema = deviceSchemas.get(i);
      if (!CLIENT_DATA_SCHEMA.containsKey(clientId)) {
        CLIENT_DATA_SCHEMA.put(clientId, new ArrayList<>());
      }
      CLIENT_DATA_SCHEMA.get(clientId).add(deviceSchema);
    }
    // TODO spricoder re-check
    return true;
  }
}
