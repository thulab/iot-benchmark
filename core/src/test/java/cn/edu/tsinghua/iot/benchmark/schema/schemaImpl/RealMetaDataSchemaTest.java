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
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link RealMetaDataSchema}'s {@code getAllFiles} walk, the most path-sensitive part
 * of real-data-set schema loading. A real data set lays out one directory per device ({@code
 * <root>/<device>/<file>.csv}) alongside top-level {@code schema.txt}/{@code info.txt} metadata.
 * The walk must key every data file by its parent-directory device name and skip the two metadata
 * files.
 */
public class RealMetaDataSchemaTest extends BenchmarkTestBase {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private File writeDeviceFile(String device) throws IOException {
    File deviceDir = folder.newFolder(device);
    File csv = new File(deviceDir, "BigBatch_0.csv");
    try (BufferedWriter writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(csv), StandardCharsets.UTF_8))) {
      writer.write("Sensor,s_0\n1000,1\n");
    }
    return csv;
  }

  @Test
  public void getAllFilesKeysEachDataFileByDeviceDirAndSkipsMetadata() throws Exception {
    File d0 = writeDeviceFile("d_0");
    File d1 = writeDeviceFile("d_1");
    // Metadata files live at the data-set root and must not be mistaken for device data files.
    assertTrue(new File(folder.getRoot(), Constants.SCHEMA_PATH).createNewFile());
    assertTrue(new File(folder.getRoot(), Constants.INFO_PATH).createNewFile());

    Method getAllFiles =
        RealMetaDataSchema.class.getDeclaredMethod("getAllFiles", String.class, Map.class);
    getAllFiles.setAccessible(true);
    Map<String, String> files = new LinkedHashMap<>();
    getAllFiles.invoke(null, folder.getRoot().getAbsolutePath(), files);

    assertEquals("only the two device files are mapped", 2, files.size());
    assertEquals(d0.getAbsolutePath(), files.get("d_0"));
    assertEquals(d1.getAbsolutePath(), files.get("d_1"));
  }
}
