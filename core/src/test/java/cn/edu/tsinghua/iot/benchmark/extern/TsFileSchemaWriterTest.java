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

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.conf.RealDatasetFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TsFileSchemaWriterTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void writesInfoTxtAndNoSchemaTxt() throws Exception {
    File dir = new File(folder.getRoot(), "ds");
    config.setREAL_DATASET_FORMAT(RealDatasetFormat.TSFILE);
    config.setFILE_PATH(dir.getAbsolutePath());

    assertTrue(SchemaWriter.getBasicWriter() instanceof TsFileSchemaWriter);
    assertTrue(new TsFileSchemaWriter().writeSchema(Collections.emptyList()));

    File info = Paths.get(dir.getAbsolutePath(), Constants.INFO_PATH).toFile();
    assertTrue(info.exists());
    assertEquals(config.toInfoText(), new String(Files.readAllBytes(info.toPath())));
    assertFalse(Paths.get(dir.getAbsolutePath(), Constants.SCHEMA_PATH).toFile().exists());
  }
}
