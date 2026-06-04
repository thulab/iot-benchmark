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
import cn.edu.tsinghua.iot.benchmark.conf.RealDatasetFormat;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.extern.TsFileDataWriter;
import cn.edu.tsinghua.iot.benchmark.extern.TsFileSchemaWriter;
import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.RealMetaDataSchema;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end: drive the production TsFile real-dataset <em>generate</em> path ({@link
 * TsFileSchemaWriter} + {@link TsFileDataWriter}) to produce a small dataset, then read it back
 * through the production <em>verification</em> path ({@link RealMetaDataSchema} + {@link
 * TsFileDataReader}) and assert the values round-trip.
 *
 * <p>The write phase runs under {@link BenchmarkMode#GENERATE_DATA}. This matters because
 * constructing any {@code SchemaWriter} eagerly forces {@code MetaDataSchema.getInstance()} (a
 * once-per-JVM singleton). Under {@code GENERATE_DATA} that resolves to {@code
 * GenerateMetaDataSchema}, which synthesizes device schemas from config; under a verification mode
 * it would instead build a {@code RealMetaDataSchema} whose constructor demands an
 * already-generated dataset and calls {@code System.exit} when one is absent — exactly the
 * generate-then-read ordering this test exercises. The read-back below does not rely on that
 * singleton: it instantiates {@code RealMetaDataSchema} directly.
 */
public class RealDatasetTsFileEndToEndTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  /**
   * {@code config} is a process-wide singleton, and {@code DataReader.getInstance} short-circuits
   * to {@link TsFileDataReader} whenever {@code REAL_DATASET_FORMAT == TSFILE}. Restore the default
   * so a later CSV/copy test in the same fork still gets its expected reader.
   */
  @After
  public void restoreDefaultFormat() {
    config.setREAL_DATASET_FORMAT(RealDatasetFormat.CSV);
  }

  @Test
  public void generatedTsFileReadsBackIdentically() throws Exception {
    config.setREAL_DATASET_FORMAT(RealDatasetFormat.TSFILE);
    config.setFILE_PATH(folder.getRoot().getAbsolutePath());
    config.setBENCHMARK_WORK_MODE(BenchmarkMode.GENERATE_DATA);
    config.setDATA_CLIENT_NUMBER(1);
    config.setSCHEMA_CLIENT_NUMBER(1);
    config.setBATCH_SIZE_PER_WRITE(100);

    List<Sensor> sensors =
        Arrays.asList(new Sensor("s_0", SensorType.INT32), new Sensor("s_1", SensorType.DOUBLE));
    DeviceSchema d0 = new DeviceSchema("g_0", "t_0", "d_0", sensors, new HashMap<>());

    // 1) write dataset metadata (clears dir + info.txt) then data
    new TsFileSchemaWriter().writeSchema(Collections.singletonList(d0));
    TsFileDataWriter writer = new TsFileDataWriter(0);
    List<Record> recs = new ArrayList<>();
    recs.add(new Record(10L, Arrays.asList((Object) 1, 0.1)));
    recs.add(new Record(20L, Arrays.asList((Object) 2, 0.2)));
    assertTrue(writer.writeBatch(new Batch(d0, recs), 0L));
    writer.close();

    // 2) read back through the production schema + reader path.
    // createMetaDataSchema() is protected in RealMetaDataSchema (a different package), so it is
    // invoked via reflection — same approach as RealMetaDataSchemaTest.
    RealMetaDataSchema schema = new RealMetaDataSchema();
    Method createMetaDataSchema =
        RealMetaDataSchema.class.getDeclaredMethod("createMetaDataSchema");
    createMetaDataSchema.setAccessible(true);
    assertTrue((boolean) createMetaDataSchema.invoke(schema));
    List<List<String>> clientFiles = MetaUtil.getClientFiles();

    List<Record> readBack = new ArrayList<>();
    TsFileDataReader reader = new TsFileDataReader(clientFiles.get(0));
    while (reader.hasNextBatch()) {
      IBatch b = reader.nextBatch();
      assertEquals("d_0", b.getDeviceSchema().getDevice());
      readBack.addAll(b.getRecords());
    }

    assertEquals(2, readBack.size());
    assertEquals(10L, readBack.get(0).getTimestamp());
    assertEquals(1, readBack.get(0).getRecordDataValue().get(0));
    assertEquals(0.1, (double) readBack.get(0).getRecordDataValue().get(1), 1e-9);
    assertEquals(20L, readBack.get(1).getTimestamp());
    assertEquals(2, readBack.get(1).getRecordDataValue().get(0));
  }
}
