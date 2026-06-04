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
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.conf.RealDatasetFormat;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.source.CSVSchemaReader;
import cn.edu.tsinghua.iot.benchmark.source.SchemaReader;
import cn.edu.tsinghua.iot.benchmark.source.TsFileSchemaReader;
import cn.edu.tsinghua.iot.benchmark.utils.CommonAlgorithms;
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
    boolean isTsFile = config.getREAL_DATASET_FORMAT() == RealDatasetFormat.TSFILE;
    SchemaReader schemaReader = isTsFile ? new TsFileSchemaReader() : new CSVSchemaReader();
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

    // Load sensor type from dataset and register device schemas (common to both formats)
    Map<String, List<Sensor>> deviceSchemaMap = schemaReader.getDeviceSchemaList();
    List<DeviceSchema> deviceSchemaList = new ArrayList<>();
    for (Map.Entry<String, List<Sensor>> device : deviceSchemaMap.entrySet()) {
      String deviceName = device.getKey();
      List<Sensor> sensors = device.getValue();
      DeviceSchema deviceSchema =
          new DeviceSchema(deviceName, sensors, MetaUtil.getTags(deviceName));
      NAME_DATA_SCHEMA.put(deviceName, deviceSchema);
      GROUPS.add(deviceSchema.getGroup());
      deviceSchemaList.add(deviceSchema);
    }

    // Split device schemas into clients (common). Pre-create a bucket for EVERY client id so that
    // clients with no devices (when client count > device count) get an empty list, not null —
    // SchemaClient/DataClient look up by id and would otherwise NPE. Mirrors
    // MetaUtil.distributeDevices.
    for (int c = 0; c < config.getSCHEMA_CLIENT_NUMBER(); c++) {
      SCHEMA_CLIENT_DATA_SCHEMA.put(c, new ArrayList<>());
    }
    for (int c = 0; c < config.getDATA_CLIENT_NUMBER(); c++) {
      DATA_CLIENT_DATA_SCHEMA.put(c, new ArrayList<>());
    }
    for (int i = 0; i < deviceSchemaList.size(); i++) {
      int schemaClientId = i % config.getSCHEMA_CLIENT_NUMBER();
      int dataClientId = i % config.getDATA_CLIENT_NUMBER();
      DeviceSchema deviceSchema = deviceSchemaList.get(i);
      SCHEMA_CLIENT_DATA_SCHEMA.get(schemaClientId).add(deviceSchema);
      DATA_CLIENT_DATA_SCHEMA.get(dataClientId).add(deviceSchema);
    }

    // Distribute data files to data clients
    List<List<String>> clientFiles = new ArrayList<>();
    for (int i = 0; i < config.getDATA_CLIENT_NUMBER(); i++) {
      clientFiles.add(new ArrayList<>());
    }
    if (isTsFile) {
      // file-level: round-robin every *.tsfile across data clients
      List<File> tsFiles = TsFileSchemaReader.listTsFiles(new File(pathStr));
      LOGGER.info("Total TsFiles: {}", tsFiles.size());
      for (int i = 0; i < tsFiles.size(); i++) {
        clientFiles.get(i % config.getDATA_CLIENT_NUMBER()).add(tsFiles.get(i).getAbsolutePath());
      }
    } else {
      // CSV: device -> single file, distributed by device count (unchanged behavior)
      Map<String, String> files = new LinkedHashMap<>();
      getAllFiles(pathStr, files);
      LOGGER.info("Total files: {}", files.size());
      Map<Integer, Integer> deviceDistributionForDataClient =
          CommonAlgorithms.distributeDevicesToClients(
              config.getDEVICE_NUMBER(), config.getDATA_CLIENT_NUMBER());
      List<Integer> deviceIds = MetaUtil.sortDeviceId();
      int index = 0;
      for (int clientId = 0; clientId < config.getDATA_CLIENT_NUMBER(); clientId++) {
        int fileNumber = deviceDistributionForDataClient.get(clientId);
        for (int fileId = 0; fileId < fileNumber; fileId++, index++) {
          String device = config.getDEVICE_NAME_PREFIX() + deviceIds.get(index);
          clientFiles.get(clientId).add(files.get(device));
        }
      }
    }
    MetaUtil.setClientFiles(clientFiles);
    return true;
  }

  private static void getAllFiles(String strPath, Map<String, String> files) {
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
        String path = f.getAbsolutePath();
        char separator = File.separatorChar;
        int lastIndexOf = path.lastIndexOf(separator);
        int secondLastIndexOf = path.lastIndexOf(separator, lastIndexOf - 1);
        if (secondLastIndexOf == -1 || lastIndexOf == -1) {
          LOGGER.error("Invalid file path: {}", path);
        } else {
          String device = path.substring(secondLastIndexOf + 1, lastIndexOf);
          files.put(device, f.getAbsolutePath());
        }
      }
    }
  }
}
