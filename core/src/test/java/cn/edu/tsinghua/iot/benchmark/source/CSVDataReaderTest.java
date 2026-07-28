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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link CSVDataReader} and the {@link DataReader#getInstance(List)} factory.
 *
 * <p>{@code CSVDataReader} reads one device per CSV file. The device name is the file's parent
 * directory; the first CSV line is a header naming the sensors; the remaining lines are {@code
 * timestamp,value,...} rows decoded according to each sensor's {@link SensorType} (the
 * value-mapping is the real-data-set "data pattern" under test). The reader looks the device's
 * sensor types up in the {@link MetaDataSchema} singleton, which this test populates by reflection
 * so the test owns the schema rather than depending on generated devices.
 */
public class CSVDataReaderTest extends BenchmarkTestBase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /** Unique device id well outside any generated range, so it never collides. */
  private static final String DEVICE = "d_8000001";

  private static final String HEADER = "Sensor,c_bool,c_int,c_long,c_float,c_double,c_text";

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private Map<String, DeviceSchema> nameDataSchema;
  private int origBatchSize;
  private boolean origCopyMode;
  private BenchmarkMode origWorkMode;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    origBatchSize = config.getBATCH_SIZE_PER_WRITE();
    origCopyMode = config.isIS_COPY_MODE();
    origWorkMode = config.getBENCHMARK_WORK_MODE();

    // CSVDataReader holds `static final metaDataSchema = MetaDataSchema.getInstance()`. Build the
    // singleton now, in a non-verification mode, so the first build picks GenerateMetaDataSchema
    // (RealMetaDataSchema would System.exit when FILE_PATH has no data set).
    config.setBENCHMARK_WORK_MODE(BenchmarkMode.TEST_WITH_DEFAULT_PATH);
    config.setIS_COPY_MODE(false);
    MetaDataSchema.getInstance();

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
    config.setBENCHMARK_WORK_MODE(origWorkMode);
  }

  /** Writes {@code <root>/<device>/<device>.csv} with the shared header and the given data rows. */
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

  /** Writes {@code <root>/<device>/<device>.csv} verbatim, so the caller can embed header lines. */
  private String writeRawDeviceCsv(String device, String content) throws IOException {
    File deviceDir = folder.newFolder(device);
    File csv = new File(deviceDir, device + ".csv");
    try (BufferedWriter writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(csv), StandardCharsets.UTF_8))) {
      writer.write(content);
    }
    return csv.getAbsolutePath();
  }

  /** Each CSV value is decoded to the Java type dictated by its sensor's {@link SensorType}. */
  @Test
  public void nextBatchDecodesEachValueToItsSensorType() throws IOException {
    config.setBATCH_SIZE_PER_WRITE(100);
    String csv =
        writeDeviceCsv(
            DEVICE, "1000,true,42,9999999999,1.5,2.5,hello", "2000,false,-7,-1,-3.5,-4.5,world");

    DataReader reader = new CSVDataReader(Collections.singletonList(csv));
    assertTrue(reader.hasNextBatch());
    IBatch batch = reader.nextBatch();

    assertEquals(DEVICE, batch.getDeviceSchema().getDevice());
    List<Record> records = batch.getRecords();
    assertEquals(2, records.size());

    Record first = records.get(0);
    assertEquals(1000L, first.getTimestamp());
    List<Object> values = first.getRecordDataValue();
    assertEquals(Boolean.TRUE, values.get(0));
    assertEquals(Integer.valueOf(42), values.get(1));
    assertEquals(Long.valueOf(9999999999L), values.get(2));
    assertEquals(Float.valueOf(1.5f), values.get(3));
    assertEquals(Double.valueOf(2.5d), values.get(4));
    assertEquals("hello", values.get(5));

    assertEquals(2000L, records.get(1).getTimestamp());

    // One file with fewer rows than BATCH_SIZE is fully drained by a single batch.
    assertFalse(reader.hasNextBatch());
  }

  /** {@code hasNextBatch}/{@code nextBatch} walk every file in order, one batch per file. */
  @Test
  public void iteratesAcrossMultipleDeviceFiles() throws IOException {
    config.setBATCH_SIZE_PER_WRITE(100);
    String fileA = writeDeviceCsv(DEVICE, "1,true,1,1,1.0,1.0,a");

    // A second device. CSVDataReader builds the batch's DeviceSchema from the directory name, so
    // the injected entry only needs to supply the matching sensor list for the type lookup.
    String otherDevice = "d_8000002";
    nameDataSchema.put(otherDevice, nameDataSchema.get(DEVICE));
    String fileB = writeDeviceCsv(otherDevice, "2,false,2,2,2.0,2.0,b");

    try {
      DataReader reader = new CSVDataReader(Arrays.asList(fileA, fileB));
      List<String> devicesSeen = new ArrayList<>();
      while (reader.hasNextBatch()) {
        devicesSeen.add(reader.nextBatch().getDeviceSchema().getDevice());
      }
      assertEquals(Arrays.asList(DEVICE, otherDevice), devicesSeen);
    } finally {
      nameDataSchema.remove(otherDevice);
    }
  }

  /** The factory wires non-copy mode to {@link CSVDataReader}. */
  @Test
  public void getInstanceReturnsCsvDataReaderInNonCopyMode() throws IOException {
    config.setIS_COPY_MODE(false);
    String csv = writeDeviceCsv(DEVICE, "1,true,1,1,1.0,1.0,a");
    DataReader reader = DataReader.getInstance(Collections.singletonList(csv));
    assertTrue(reader instanceof CSVDataReader);
  }

  /** STRING, BLOB, DATE and TIMESTAMP decode to String, String, {@link LocalDate} and Long. */
  @Test
  public void nextBatchDecodesExtendedSensorTypes() throws IOException {
    config.setBATCH_SIZE_PER_WRITE(100);
    String device = "d_8000004";
    nameDataSchema.put(
        device,
        new DeviceSchema(
            device,
            Arrays.asList(
                new Sensor("s_str", SensorType.STRING),
                new Sensor("s_blob", SensorType.BLOB),
                new Sensor("s_date", SensorType.DATE),
                new Sensor("s_ts", SensorType.TIMESTAMP)),
            MetaUtil.getTags(device)));
    try {
      String csv =
          writeRawDeviceCsv(
              device,
              "Sensor,s_str,s_blob,s_date,s_ts\n" + "1000,hello,bytes,2024-01-01,123456789\n");

      DataReader reader = new CSVDataReader(Collections.singletonList(csv));
      assertTrue(reader.hasNextBatch());
      List<Object> values = reader.nextBatch().getRecords().get(0).getRecordDataValue();

      assertEquals("hello", values.get(0));
      assertEquals("bytes", values.get(1));
      assertEquals(LocalDate.of(2024, 1, 1), values.get(2));
      assertEquals(123456789L, values.get(3));
    } finally {
      nameDataSchema.remove(device);
    }
  }

  /**
   * {@code CSVDataWriter} re-emits the {@code Sensor} header before every written batch, so one
   * device file holds several header-delimited segments. With {@code BATCH_SIZE} aligned to a
   * segment's row count, the reader yields one batch per segment from the single file.
   */
  @Test
  public void drainsHeaderDelimitedSegmentsAsSeparateBatches() throws IOException {
    config.setBATCH_SIZE_PER_WRITE(2);
    String csv =
        writeRawDeviceCsv(
            DEVICE,
            HEADER
                + "\n"
                + "1000,true,1,1,1.0,1.0,a\n"
                + "2000,false,2,2,2.0,2.0,b\n"
                + HEADER
                + "\n"
                + "3000,true,3,3,3.0,3.0,c\n"
                + "4000,false,4,4,4.0,4.0,d\n");

    DataReader reader = new CSVDataReader(Collections.singletonList(csv));

    assertTrue(reader.hasNextBatch());
    List<Record> first = reader.nextBatch().getRecords();
    assertEquals(2, first.size());
    assertEquals(1000L, first.get(0).getTimestamp());
    assertEquals(2000L, first.get(1).getTimestamp());

    assertTrue(reader.hasNextBatch());
    List<Record> second = reader.nextBatch().getRecords();
    assertEquals(2, second.size());
    assertEquals(3000L, second.get(0).getTimestamp());
    assertEquals(4000L, second.get(1).getTimestamp());

    assertFalse(reader.hasNextBatch());
  }
}
