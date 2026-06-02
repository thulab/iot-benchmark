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
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link CSVSchemaReader}, the reader that loads the schema of a real data set.
 *
 * <p>A real data set directory carries a {@code schema.txt} (one {@code device sensor typeOrdinal}
 * triple per line) describing every device's sensors, and an {@code info.txt} snapshot of the
 * generation config used to validate that the running benchmark matches the data set. Both files
 * are resolved relative to {@code config.FILE_PATH}; this test points that at a temp folder.
 */
public class CSVSchemaReaderTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private String origFilePath;

  @Before
  public void setUp() {
    origFilePath = config.getFILE_PATH();
    config.setFILE_PATH(folder.getRoot().getAbsolutePath());
  }

  @After
  public void tearDown() {
    config.setFILE_PATH(origFilePath);
  }

  private void writeDataSetFile(String name, String content) throws IOException {
    Files.write(
        Paths.get(folder.getRoot().getAbsolutePath(), name),
        content.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * {@code getDeviceSchemaList} must group sensors under their device, keep insertion order, decode
   * the third column as a {@link SensorType} ordinal, and skip blank lines.
   */
  @Test
  public void getDeviceSchemaListParsesDevicesSensorsAndTypes() throws IOException {
    // typeOrdinal: 0=BOOLEAN, 1=INT32, 4=DOUBLE, 5=TEXT (see SensorType declaration order)
    writeDataSetFile(
        Constants.SCHEMA_PATH,
        "d_0 s_0 1\n" + "d_0 s_1 4\n" + "\n" + "d_1 s_0 0\n" + "d_1 s_1 5\n");

    Map<String, List<Sensor>> result = new CSVSchemaReader().getDeviceSchemaList();

    assertEquals("two distinct devices expected", 2, result.size());
    // LinkedHashMap preserves the order devices first appear in the file.
    Iterator<String> deviceOrder = result.keySet().iterator();
    assertEquals("d_0", deviceOrder.next());
    assertEquals("d_1", deviceOrder.next());

    List<Sensor> d0 = result.get("d_0");
    assertEquals(2, d0.size());
    assertEquals("s_0", d0.get(0).getName());
    assertEquals(SensorType.INT32, d0.get(0).getSensorType());
    assertEquals("s_1", d0.get(1).getName());
    assertEquals(SensorType.DOUBLE, d0.get(1).getSensorType());

    List<Sensor> d1 = result.get("d_1");
    assertEquals(2, d1.size());
    assertEquals(SensorType.BOOLEAN, d1.get(0).getSensorType());
    assertEquals(SensorType.TEXT, d1.get(1).getSensorType());
  }

  /** With no {@code info.txt} present the data set cannot be validated. */
  @Test
  public void checkDataSetReturnsFalseWhenInfoFileMissing() {
    assertFalse(new CSVSchemaReader().checkDataSet());
  }

  /** An {@code info.txt} equal to the current config's snapshot validates successfully. */
  @Test
  public void checkDataSetReturnsTrueWhenInfoMatchesConfig() throws IOException {
    writeDataSetFile(Constants.INFO_PATH, config.toInfoText());
    assertTrue(new CSVSchemaReader().checkDataSet());
  }

  /** A single differing line (same line count) fails validation. */
  @Test
  public void checkDataSetReturnsFalseWhenInfoDiffersFromConfig() throws IOException {
    String info = config.toInfoText();
    String mutated = info.replaceFirst("LOOP=\\d+", "LOOP=" + (config.getLOOP() + 987654));
    assertNotEquals("mutation must actually change the snapshot", info, mutated);
    writeDataSetFile(Constants.INFO_PATH, mutated);
    assertFalse(new CSVSchemaReader().checkDataSet());
  }
}
