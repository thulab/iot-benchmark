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

package cn.edu.tsinghua.iot.benchmark.mode.enums;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum BenchmarkMode {
  TEST_WITH_DEFAULT_PATH("testWithDefaultPath"),
  GENERATE_DATA("generateDataMode"),
  VERIFICATION_WRITE("verificationWriteMode"),
  VERIFICATION_QUERY("verificationQueryMode"),
  SERVER_MODE("serverMode");

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkMode.class);
  public String mode;

  BenchmarkMode(String mode) {
    mode = mode.trim();
    this.mode = mode;
  }

  public static BenchmarkMode getBenchmarkMode(String mode) {
    for (BenchmarkMode benchmarkMode : BenchmarkMode.values()) {
      if (benchmarkMode.mode.equals(mode)) {
        return benchmarkMode;
      }
    }
    BenchmarkMode defaultBenchmark = BenchmarkMode.TEST_WITH_DEFAULT_PATH;
    LOGGER.warn("Using Benchmark Mode: " + defaultBenchmark.mode);
    return defaultBenchmark;
  }

  @Override
  public String toString() {
    return mode;
  }
}
