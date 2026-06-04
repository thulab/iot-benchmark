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

package cn.edu.tsinghua.iot.benchmark.source;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SchemaReaderCheckDataSetTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  /**
   * A shorter info.txt (e.g. an older dataset with fewer config lines) must return false, not
   * throw.
   */
  @Test
  public void shortInfoTxtReturnsFalseInsteadOfThrowing() throws Exception {
    config.setFILE_PATH(folder.getRoot().getAbsolutePath());
    Files.write(
        Paths.get(folder.getRoot().getAbsolutePath(), Constants.INFO_PATH),
        "LOOP=1".getBytes(StandardCharsets.UTF_8));

    assertFalse(new CSVSchemaReader().checkDataSet());
  }

  @Test
  public void matchingInfoTxtReturnsTrue() throws Exception {
    config.setFILE_PATH(folder.getRoot().getAbsolutePath());
    Files.write(
        Paths.get(folder.getRoot().getAbsolutePath(), Constants.INFO_PATH),
        config.toInfoText().getBytes(StandardCharsets.UTF_8));

    assertTrue(new CSVSchemaReader().checkDataSet());
  }
}
