/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.iotdb200;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy.SessionStrategy;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBSwitch;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that {@code SessionStrategy.genTablet} marks null cells (sparse matrix write,
 * NULL_RATIO) in the tablet BitMaps for both the tree model and the table model.
 */
public class SessionStrategyNullTabletTest {
  static {
    System.setProperty(
        "benchmark-conf",
        Paths.get("..", "configuration", "conf").toAbsolutePath().normalize().toString());
  }

  private static final Config CONFIG = ConfigDescriptor.getInstance().getConfig();

  private SQLDialect originalDialect;
  private int originalDeviceNumPerWrite;
  private boolean originalDoubleWrite;

  @Before
  public void setUp() {
    originalDialect = CONFIG.getIoTDB_DIALECT_MODE();
    originalDeviceNumPerWrite = CONFIG.getDEVICE_NUM_PER_WRITE();
    originalDoubleWrite = CONFIG.isIS_DOUBLE_WRITE();
    CONFIG.setIS_DOUBLE_WRITE(false);
    CONFIG.setDEVICE_NUM_PER_WRITE(1);
  }

  @After
  public void tearDown() {
    CONFIG.setIoTDB_DIALECT_MODE(originalDialect);
    CONFIG.setDEVICE_NUM_PER_WRITE(originalDeviceNumPerWrite);
    CONFIG.setIS_DOUBLE_WRITE(originalDoubleWrite);
  }

  /** Row 0 has a null measurement cell, row 1 is fully populated. */
  private Batch newBatch() {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("region", "beijing");
    DeviceSchema schema =
        new DeviceSchema(
            "0",
            "0",
            "d_0",
            Arrays.asList(
                new Sensor("s_0", SensorType.DOUBLE),
                new Sensor("s_1", SensorType.INT64),
                new Sensor("s_2", SensorType.TEXT)),
            tags);
    return new Batch(
        schema,
        Arrays.asList(
            new Record(1L, new ArrayList<>(Arrays.asList(null, 10L, "v1"))),
            new Record(2L, new ArrayList<>(Arrays.asList(2.5D, 20L, "v2")))));
  }

  private Tablet genTablet(SQLDialect dialect, Batch batch) throws Exception {
    CONFIG.setIoTDB_DIALECT_MODE(dialect);
    DBConfig dbConfig = new DBConfig();
    dbConfig.setDB_SWITCH(DBSwitch.DB_IOT_200_SESSION_BY_TABLET);
    dbConfig.setDB_NAME("benchmark");
    // The protected constructor only initializes the model strategy; genTablet never touches
    // the DML strategy, so no session connection is required.
    Constructor<IoTDB> constructor =
        IoTDB.class.getDeclaredConstructor(DBConfig.class, boolean.class);
    constructor.setAccessible(true);
    IoTDB iotdb = constructor.newInstance(dbConfig, false);
    Method method = SessionStrategy.class.getDeclaredMethod("genTablet", IoTDB.class, IBatch.class);
    method.setAccessible(true);
    return (Tablet) method.invoke(null, iotdb, batch);
  }

  private BitMap[] bitMapsOf(Tablet tablet) throws Exception {
    Field bitMapsField = Tablet.class.getDeclaredField("bitMaps");
    bitMapsField.setAccessible(true);
    BitMap[] bitMaps = (BitMap[]) bitMapsField.get(tablet);
    assertNotNull("tablet bitMaps must be initialized for sparse writes", bitMaps);
    return bitMaps;
  }

  @Test
  public void treeModelMarksNullCellInBitMap() throws Exception {
    Batch batch = newBatch();
    Tablet tablet = genTablet(SQLDialect.TREE, batch);
    BitMap[] bitMaps = bitMapsOf(tablet);

    // s_0 of row 0 is null -> marked, other cells are not
    assertTrue("null measurement cell must be marked in the tree model", bitMaps[0].isMarked(0));
    assertFalse(bitMaps[1].isMarked(0));
    assertFalse(bitMaps[2].isMarked(0));
    for (int i = 0; i < 3; i++) {
      assertFalse("fully populated row must have no null marks", bitMaps[i].isMarked(1));
    }
  }

  @Test
  public void tableModelMarksNullCellInBitMap() throws Exception {
    Batch batch = newBatch();
    Tablet tablet = genTablet(SQLDialect.TABLE, batch);
    BitMap[] bitMaps = bitMapsOf(tablet);

    // table model: s_0, s_1, s_2 are FIELD columns; device_id and region are ID columns appended
    // after them. Only the null measurement cell of row 0 may be marked.
    assertEquals(
        "tablet must include the 3 measurement columns plus the device_id and region ID columns",
        5,
        bitMaps.length);
    assertTrue("null measurement cell must be marked in the table model", bitMaps[0].isMarked(0));
    for (int column = 1; column < bitMaps.length; column++) {
      assertFalse("non-null cell must not be marked", bitMaps[column].isMarked(0));
      assertFalse("non-null cell must not be marked", bitMaps[column].isMarked(1));
    }
  }
}
