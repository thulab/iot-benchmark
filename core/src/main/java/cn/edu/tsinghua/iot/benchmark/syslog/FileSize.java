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

package cn.edu.tsinghua.iot.benchmark.syslog;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class FileSize {
  private static final Logger log = LoggerFactory.getLogger(FileSize.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String LINUX_FILE_SIZE_CMD = "du -sm %s";
  private static final float MB2GB = 1024;
  private static final float ABNORMAL_VALUE = -1;

  private FileSize() {
    switch (config.getDbConfig().getDB_SWITCH().getType()) {
      case IoTDB:
        break;
      default:
        log.error("unsupported db name: {}", config.getDbConfig().getDB_SWITCH());
    }
  }

  public static FileSize getInstance() {
    return FileSizeHolder.INSTANCE;
  }

  /** Use `du` to get File size */
  public Map<FileSizeKinds, Float> getFileSize() {
    Map<FileSizeKinds, Float> fileSize = new EnumMap<>(FileSizeKinds.class);
    BufferedReader in;
    Process pro = null;
    Runtime runtime = Runtime.getRuntime();
    for (FileSizeKinds kinds : FileSizeKinds.values()) {
      float fileSizeGB = ABNORMAL_VALUE;
      for (String path_ : kinds.path) {
        String command = String.format(LINUX_FILE_SIZE_CMD, path_);
        try {
          pro = runtime.exec(command);
          in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
          String line;
          while ((line = in.readLine()) != null) {
            String size = line.split("\\s+")[0];
            if (fileSizeGB == ABNORMAL_VALUE) {
              fileSizeGB = 0;
            }
            fileSizeGB += Float.parseFloat(size) / MB2GB;
          }
          in.close();
        } catch (IOException e) {
          log.info("Execute command failed: {}", command);
        }
      }
      fileSize.put(kinds, fileSizeGB);
    }
    if (pro != null) {
      pro.destroy();
    }
    return fileSize;
  }

  /** Describe different kind files */
  public enum FileSizeKinds {
    DATA(config.getIOTDB_DATA_DIR()),
    SYSTEM(config.getIOTDB_SYSTEM_DIR()),
    WAL(config.getIOTDB_WAL_DIR()),
    SEQUENCE(config.getSEQUENCE_DIR()),
    UN_SEQUENCE(config.getUNSEQUENCE_DIR());

    List<String> path;

    FileSizeKinds(List<String> path) {
      this.path = path;
    }
  }

  private static class FileSizeHolder {
    private static final FileSize INSTANCE = new FileSize();
  }
}
