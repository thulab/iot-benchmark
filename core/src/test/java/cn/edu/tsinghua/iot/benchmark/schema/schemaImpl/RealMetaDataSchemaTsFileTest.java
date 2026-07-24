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

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.conf.RealDatasetFormat;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.source.DataReader;
import cn.edu.tsinghua.iot.benchmark.source.TsFileDataReader;
import cn.edu.tsinghua.iot.benchmark.source.TsFileTestFixtures;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RealMetaDataSchemaTsFileTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void registersDevicesAndDistributesFilesForTsFile() throws Exception {
    File data = new File(folder.getRoot(), "data_0.tsfile");
    TsFileTestFixtures.writeSampleTable(data);
    config.setREAL_DATASET_FORMAT(RealDatasetFormat.TSFILE);
    config.setFILE_PATH(folder.getRoot().getAbsolutePath());
    config.setBENCHMARK_WORK_MODE(BenchmarkMode.VERIFICATION_WRITE);
    config.setDATA_CLIENT_NUMBER(1);
    config.setSCHEMA_CLIENT_NUMBER(1);
    // info.txt must match current config for checkDataSet to pass.
    Files.write(
        Paths.get(folder.getRoot().getAbsolutePath(), Constants.INFO_PATH),
        config.toInfoText().getBytes(StandardCharsets.UTF_8));

    RealMetaDataSchema schema = new RealMetaDataSchema();
    assertTrue(schema.createMetaDataSchema());

    List<List<String>> clientFiles = MetaUtil.getClientFiles();
    assertEquals(1, clientFiles.size());
    assertEquals(1, clientFiles.get(0).size());
    assertTrue(clientFiles.get(0).get(0).endsWith("data_0.tsfile"));

    DataReader reader = DataReader.getInstance(clientFiles.get(0));
    assertTrue(reader instanceof TsFileDataReader);
    int rows = 0;
    while (reader.hasNextBatch()) {
      IBatch b = reader.nextBatch();
      rows += b.getRecords().size();
    }
    assertEquals(4, rows);
  }

  /**
   * Regression: when client count exceeds device count, every schema/data client id must still get
   * a (possibly empty) device list, not null — otherwise SchemaClient NPEs during registration.
   */
  @Test
  public void everyClientGetsNonNullBucketWhenClientCountExceedsDevices() throws Exception {
    // fixture has 2 devices (d_0, d_1); use 20 clients so ids 2..19 receive no device
    File data = new File(folder.getRoot(), "data_0.tsfile");
    TsFileTestFixtures.writeSampleTable(data);
    config.setREAL_DATASET_FORMAT(RealDatasetFormat.TSFILE);
    config.setFILE_PATH(folder.getRoot().getAbsolutePath());
    config.setBENCHMARK_WORK_MODE(BenchmarkMode.VERIFICATION_WRITE);
    config.setDATA_CLIENT_NUMBER(20);
    config.setSCHEMA_CLIENT_NUMBER(20);
    Files.write(
        Paths.get(folder.getRoot().getAbsolutePath(), Constants.INFO_PATH),
        config.toInfoText().getBytes(StandardCharsets.UTF_8));

    RealMetaDataSchema schema = new RealMetaDataSchema();
    assertTrue(schema.createMetaDataSchema());

    for (int id = 0; id < 20; id++) {
      assertNotNull("schema client " + id, schema.getDeviceSchemaBySchemaClientId(id));
      assertNotNull("data client " + id, schema.getDeviceSchemaByDataClientId(id));
    }
  }
}
