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
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iot.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link CopyDataReader} and the copy-mode branch of {@link
 * DataReader#getInstance(List)}.
 *
 * <p>Copy mode reads a single device file once at construction and then re-emits that batch {@code
 * LOOP} times, advancing every timestamp by the file's time span ({@code last - first}) on each
 * loop so the synthetic stream marches forward in time without re-reading from disk.
 */
public class CopyDataReaderTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String DEVICE = "d_8000003";
  private static final String HEADER = "Sensor,c_bool,c_int,c_long,c_float,c_double,c_text";

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private Map<String, DeviceSchema> nameDataSchema;
  private int origBatchSize;
  private boolean origCopyMode;
  private boolean origAnomaly;
  private double origAnomalyRate;
  private int origAnomalyTimes;
  private long origLoop;
  private BenchmarkMode origWorkMode;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    origBatchSize = config.getBATCH_SIZE_PER_WRITE();
    origCopyMode = config.isIS_COPY_MODE();
    origAnomaly = config.isIS_ADD_ANOMALY();
    origAnomalyRate = config.getANOMALY_RATE();
    origAnomalyTimes = config.getANOMALY_TIMES();
    origLoop = config.getLOOP();
    origWorkMode = config.getBENCHMARK_WORK_MODE();

    config.setBENCHMARK_WORK_MODE(BenchmarkMode.TEST_WITH_DEFAULT_PATH);
    MetaDataSchema.getInstance();
    config.setIS_COPY_MODE(true);
    config.setIS_ADD_ANOMALY(false);
    config.setBATCH_SIZE_PER_WRITE(100);

    Field field = MetaDataSchema.class.getDeclaredField("NAME_DATA_SCHEMA");
    field.setAccessible(true);
    nameDataSchema = (Map<String, DeviceSchema>) field.get(null);

    List<Sensor> sensors =
        Arrays.asList(
            new Sensor("c_bool", SensorType.BOOLEAN),
            new Sensor("c_int", SensorType.INT32),
            new Sensor("c_long", SensorType.INT64),
            new Sensor("c_float", SensorType.FLOAT),
            new Sensor("c_double", SensorType.DOUBLE),
            new Sensor("c_text", SensorType.TEXT));
    nameDataSchema.put(DEVICE, new DeviceSchema(DEVICE, sensors, MetaUtil.getTags(DEVICE)));
  }

  @After
  public void tearDown() {
    nameDataSchema.remove(DEVICE);
    config.setBATCH_SIZE_PER_WRITE(origBatchSize);
    config.setIS_COPY_MODE(origCopyMode);
    config.setIS_ADD_ANOMALY(origAnomaly);
    config.setANOMALY_RATE(origAnomalyRate);
    config.setANOMALY_TIMES(origAnomalyTimes);
    config.setLOOP(origLoop);
    config.setBENCHMARK_WORK_MODE(origWorkMode);
  }

  private String writeDeviceCsv(String device, String... rows) throws IOException {
    File deviceDir = folder.newFolder(device);
    File csv = new File(deviceDir, device + ".csv");
    try (BufferedWriter writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(csv), StandardCharsets.UTF_8))) {
      writer.write(HEADER);
      writer.write("\n");
      for (String row : rows) {
        writer.write(row);
        writer.write("\n");
      }
    }
    return csv.getAbsolutePath();
  }

  /** {@code hasNextBatch} yields exactly {@code LOOP} batches, then stops. */
  @Test
  public void emitsExactlyLoopBatches() throws IOException {
    config.setLOOP(3);
    String csv = writeDeviceCsv(DEVICE, "1000,true,1,1,1.0,1.0,a", "3000,false,2,2,2.0,2.0,b");

    CopyDataReader reader = new CopyDataReader(Collections.singletonList(csv));
    int batches = 0;
    while (reader.hasNextBatch()) {
      reader.nextBatch();
      batches++;
    }
    assertEquals(3, batches);
    assertFalse(reader.hasNextBatch());
  }

  /** Each loop advances every record's timestamp by the file's (last - first) span. */
  @Test
  public void shiftsTimestampsByFileSpanOnEachLoop() throws IOException {
    config.setLOOP(2);
    // span = 3000 - 1000 = 2000
    String csv = writeDeviceCsv(DEVICE, "1000,true,1,1,1.0,1.0,a", "3000,false,2,2,2.0,2.0,b");

    CopyDataReader reader = new CopyDataReader(Collections.singletonList(csv));

    IBatch first = reader.nextBatch();
    long firstRowAfterLoop1 = first.getRecords().get(0).getTimestamp();
    long secondRowAfterLoop1 = first.getRecords().get(1).getTimestamp();
    assertEquals(3000L, firstRowAfterLoop1);
    assertEquals(5000L, secondRowAfterLoop1);

    // nextBatch re-emits and mutates the same batch instance, advancing by the span again.
    IBatch second = reader.nextBatch();
    assertEquals(5000L, second.getRecords().get(0).getTimestamp());
    assertEquals(7000L, second.getRecords().get(1).getTimestamp());
  }

  /** The factory wires copy mode to {@link CopyDataReader}. */
  @Test
  public void getInstanceReturnsCopyDataReaderInCopyMode() throws IOException {
    config.setLOOP(1);
    String csv = writeDeviceCsv(DEVICE, "1000,true,1,1,1.0,1.0,a");
    DataReader reader = DataReader.getInstance(Collections.singletonList(csv));
    assertTrue(reader instanceof CopyDataReader);
  }

  /**
   * With anomaly injection on and a 100% rate, every emitted record is multiplied: numeric values
   * scale by {@code ANOMALY_TIMES} while BOOLEAN/TEXT values and the per-loop timestamp shift are
   * preserved.
   */
  @Test
  public void injectsAnomaliesIntoNumericValuesWhenEnabled() throws IOException {
    config.setLOOP(1);
    config.setIS_ADD_ANOMALY(true);
    config.setANOMALY_RATE(1.0);
    config.setANOMALY_TIMES(2);
    // span = 3000 - 1000 = 2000
    String csv = writeDeviceCsv(DEVICE, "1000,true,1,2,3.0,4.0,x", "3000,false,5,6,7.0,8.0,y");

    CopyDataReader reader = new CopyDataReader(Collections.singletonList(csv));
    List<Record> records = reader.nextBatch().getRecords();
    assertEquals(2, records.size());

    List<Object> first = records.get(0).getRecordDataValue();
    assertEquals(3000L, records.get(0).getTimestamp());
    assertEquals(Boolean.TRUE, first.get(0));
    assertEquals(Integer.valueOf(2), first.get(1));
    assertEquals(Long.valueOf(4), first.get(2));
    assertEquals(Float.valueOf(6.0f), first.get(3));
    assertEquals(Double.valueOf(8.0d), first.get(4));
    assertEquals("x", first.get(5));

    List<Object> second = records.get(1).getRecordDataValue();
    assertEquals(5000L, records.get(1).getTimestamp());
    assertEquals(Boolean.FALSE, second.get(0));
    assertEquals(Integer.valueOf(10), second.get(1));
    assertEquals(Long.valueOf(12), second.get(2));
    assertEquals(Float.valueOf(14.0f), second.get(3));
    assertEquals(Double.valueOf(16.0d), second.get(4));
    assertEquals("y", second.get(5));
  }
}
