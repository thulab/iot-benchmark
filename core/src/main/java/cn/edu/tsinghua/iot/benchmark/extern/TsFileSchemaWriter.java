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

package cn.edu.tsinghua.iot.benchmark.extern;

import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Generates a TsFile real dataset's metadata: clears FILE_PATH and writes info.txt (no schema.txt).
 */
public class TsFileSchemaWriter extends SchemaWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileSchemaWriter.class);

  @Override
  public boolean writeSchema(List<DeviceSchema> deviceSchemaList) {
    try {
      Path path = Paths.get(config.getFILE_PATH());
      if (Files.isDirectory(path)) {
        try (Stream<Path> walk = Files.walk(path)) {
          walk.sorted(Comparator.reverseOrder())
              .forEach(
                  subPath -> {
                    try {
                      Files.delete(subPath);
                    } catch (IOException e) {
                      LOGGER.error("Failed to delete {}", subPath, e);
                    }
                  });
        }
      } else {
        Files.deleteIfExists(path);
      }
      Files.createDirectories(path);

      Path infoPath = Paths.get(FileUtils.union(config.getFILE_PATH(), Constants.INFO_PATH));
      Files.createFile(infoPath);
      Files.write(infoPath, config.toInfoText().getBytes(StandardCharsets.UTF_8));
      LOGGER.info("Finish writing TsFile dataset info to {}", infoPath);
      return true;
    } catch (IOException ioException) {
      LOGGER.error(
          "Failed to generate TsFile dataset metadata in {}", config.getFILE_PATH(), ioException);
      return false;
    }
  }
}
