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

package cn.edu.tsinghua.iot.benchmark;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.csv.CSVShutdownHook;
import cn.edu.tsinghua.iot.benchmark.mode.BaseMode;
import cn.edu.tsinghua.iot.benchmark.mode.GenerateDataMode;
import cn.edu.tsinghua.iot.benchmark.mode.TestWithDefaultPathMode;
import cn.edu.tsinghua.iot.benchmark.mode.VerificationQueryMode;
import cn.edu.tsinghua.iot.benchmark.mode.VerificationWriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class App {
  private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

  public static void main(String[] args) throws SQLException {
    long initialHeapSize = Runtime.getRuntime().totalMemory();
    long maxHeapSize = Runtime.getRuntime().maxMemory();
    LOGGER.info(
        "Initial Heap Size: {} bytes, Max Heap Size: {} bytes. ", initialHeapSize, maxHeapSize);

    if (args == null || args.length == 0) {
      args = new String[] {"-cf", "configuration/conf"};
    }
    CommandCli cli = new CommandCli();
    if (!cli.init(args)) {
      return;
    }
    Runtime.getRuntime().addShutdownHook(new CSVShutdownHook());
    Config config = ConfigDescriptor.getInstance().getConfig();
    BaseMode baseMode;
    switch (config.getBENCHMARK_WORK_MODE()) {
      case TEST_WITH_DEFAULT_PATH:
        baseMode = new TestWithDefaultPathMode();
        break;
      case GENERATE_DATA:
        baseMode = new GenerateDataMode();
        break;
      case VERIFICATION_WRITE:
        baseMode = new VerificationWriteMode();
        break;
      case VERIFICATION_QUERY:
        baseMode = new VerificationQueryMode();
        break;
      default:
        throw new SQLException("Unsupported mode:" + config.getBENCHMARK_WORK_MODE());
    }
    baseMode.run();
  }
}
