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
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class RealDataSchema extends BaseDataSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealDataSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /** Create Data Schema for each device */
  @Override
  protected void createDataSchema() {
    Path path = Paths.get(config.getFILE_PATH());
    if (!Files.exists(path)) {
      LOGGER.error("{} dataset does not exit", config.getFILE_PATH());
      return;
    }

    List<String> files = new ArrayList<>();
    getAllFiles(config.getFILE_PATH(), files);
    LOGGER.info("Total files: {}", files.size());
    Collections.sort(files);

    Map<String, Map<String, Type>> deviceSchemaMap = BasicReader.getDeviceSchemaList();
    List<DeviceSchema> deviceSchemaList = new ArrayList<>();
    for (Map.Entry<String, Map<String, Type>> device : deviceSchemaMap.entrySet()) {
      String deviceName = device.getKey();
      List<String> sensors = new ArrayList<>(device.getValue().keySet());
      sensors.sort(
          new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
              return Integer.valueOf(o1.replace(Constants.SENSOR_NAME_PREFIX, ""))
                  - Integer.valueOf(o2.replace(Constants.SENSOR_NAME_PREFIX, ""));
            }
          });
      DeviceSchema deviceSchema =
          new DeviceSchema(MetaUtil.getGroupNameByDeviceStr(deviceName), deviceName, sensors);
      addSensorType(MetaUtil.getDeviceName(deviceName), device.getValue());
      deviceSchemaList.add(deviceSchema);
    }

    // Split into Thread And store Type
    for (int i = 0; i < deviceSchemaList.size(); i++) {
      int threadId = i % config.getCLIENT_NUMBER();
      DeviceSchema deviceSchema = deviceSchemaList.get(i);
      if (!CLIENT_BIND_SCHEMA.containsKey(threadId)) {
        CLIENT_BIND_SCHEMA.put(threadId, new ArrayList<>());
      }
      CLIENT_BIND_SCHEMA.get(threadId).add(deviceSchema);
    }

    // Split Files into Thread
    List<List<String>> threadFiles = new ArrayList<>();
    for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
      threadFiles.add(new ArrayList<>());
    }

    for (int i = 0; i < files.size(); i++) {
      String filePath = files.get(i);
      int thread = i % config.getCLIENT_NUMBER();
      threadFiles.get(thread).add(filePath);
    }
    MetaUtil.setThreadFiles(threadFiles);
    // set up loop
    this.loopPerClient = files.size() / config.getCLIENT_NUMBER();
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
