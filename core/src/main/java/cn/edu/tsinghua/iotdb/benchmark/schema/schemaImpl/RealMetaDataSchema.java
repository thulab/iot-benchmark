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
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.source.CSVSchemaReader;
import cn.edu.tsinghua.iotdb.benchmark.source.SchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class RealMetaDataSchema extends MetaDataSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealMetaDataSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  protected boolean createMetaDataSchema() {
    SchemaReader schemaReader = new CSVSchemaReader();
    String pathStr = config.getFILE_PATH();
    Path path = Paths.get(pathStr);
    // Check the existence of dataset
    if (!Files.exists(path)) {
      LOGGER.error("{} dataset does not exit", config.getFILE_PATH());
      return false;
    }
    // Check the validation of config between benchmark and dataset
    if (!config.isIS_COPY_MODE() && !schemaReader.checkDataSet()) {
      LOGGER.error("There are difference between benchmark and dataset");
      return false;
    }
    // Load file from dataset
    List<String> files = new ArrayList<>();
    getAllFiles(pathStr, files);
    LOGGER.info("Total files: {}", files.size());
    Collections.sort(files);

    // Load sensor type from dataset
    Map<String, List<Sensor>> deviceSchemaMap = schemaReader.getDeviceSchemaList();
    List<DeviceSchema> deviceSchemaList = new ArrayList<>();
    for (Map.Entry<String, List<Sensor>> device : deviceSchemaMap.entrySet()) {
      String deviceName = device.getKey();
      List<Sensor> sensors = sortSensors(device.getValue());
      DeviceSchema deviceSchema =
          new DeviceSchema(
              MetaUtil.getGroupIdFromDeviceName(deviceName),
              deviceName,
              sensors,
              config.getDEVICE_TAGS());
      NAME_DATA_SCHEMA.put(deviceName, deviceSchema);
      deviceSchemaList.add(deviceSchema);
    }

    // Split into client And store Type
    for (int i = 0; i < deviceSchemaList.size(); i++) {
      int clientId = i % config.getCLIENT_NUMBER();
      DeviceSchema deviceSchema = deviceSchemaList.get(i);
      if (!CLIENT_DATA_SCHEMA.containsKey(clientId)) {
        CLIENT_DATA_SCHEMA.put(clientId, new ArrayList<>());
      }
      CLIENT_DATA_SCHEMA.get(clientId).add(deviceSchema);
    }

    // Split data files into client
    List<List<String>> clientFiles = new ArrayList<>();
    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      clientFiles.add(new ArrayList<>());
    }

    for (int i = 0; i < files.size(); i++) {
      String filePath = files.get(i);
      int clientId = i % config.getCLIENT_NUMBER();
      clientFiles.get(clientId).add(filePath);
    }
    MetaUtil.setClientFiles(clientFiles);
    return true;
  }

  private static void getAllFiles(String strPath, List<String> files) {
    File f = new File(strPath);
    if (f.isDirectory()) {
      File[] fs = f.listFiles();
      assert fs != null;
      for (File f1 : fs) {
        String fsPath = f1.getAbsolutePath();
        getAllFiles(fsPath, files);
      }
    } else if (f.isFile()) {
      if (!f.getAbsolutePath().contains(Constants.SCHEMA_PATH)
          && !f.getAbsolutePath().contains(Constants.INFO_PATH)) {
        files.add(f.getAbsolutePath());
      }
    }
  }
}
