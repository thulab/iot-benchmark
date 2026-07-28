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

import cn.edu.tsinghua.iot.benchmark.conf.Constants;

import java.io.File;

/**
 * Base class for every test that directly or transitively touches {@code ConfigDescriptor}.
 *
 * <p>{@code Config.initInnerFunction()} reads {@code function.xml} from the directory named by the
 * {@code benchmark-conf} system property (default {@code configuration/conf}) and calls {@code
 * System.exit(0)} when the file is missing. Under {@code mvn test -pl core} the surefire fork's
 * working directory is the {@code core} module directory, where {@code configuration/conf} does not
 * exist, so config-touching tests would silently abort.
 *
 * <p>This static initializer points the property at the repository's {@code configuration/conf}
 * (resolved as {@code ../configuration/conf} from the core module dir). The JVM runs a superclass
 * static initializer before any subclass static field initializer, so the property is set before a
 * subclass's {@code static Config config = ConfigDescriptor.getInstance()...} runs. An externally
 * supplied {@code -Dbenchmark-conf} is preserved.
 */
public abstract class BenchmarkTestBase {
  static {
    if (System.getProperty(Constants.BENCHMARK_CONF) == null) {
      System.setProperty(
          Constants.BENCHMARK_CONF, new File("../configuration/conf").getAbsolutePath());
    }
  }
}
