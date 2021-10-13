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

package cn.edu.tsinghua.iotdb.benchmark.serialize;

import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BatchSerializeTest {

  @Before
  public void before() throws Exception {}

  @After
  public void after() throws Exception {}

  @Test
  public void testSerialize() throws Exception {
    String group = "g1";
    String device = "d1";
    List<Sensor> sensors = new ArrayList<>();
    sensors.add(new Sensor("s1", SensorType.DOUBLE));
    sensors.add(new Sensor("s2", SensorType.DOUBLE));
    DeviceSchema deviceSchema = new DeviceSchema(group, device, sensors);
    List<Record> records = new LinkedList<>();
    for (int i = 0; i < 12; i++) {
      records.add(buildRecord(i, 10));
    }

    Batch batch = new Batch(deviceSchema, records);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    batch.serialize(outputStream);
    ByteArrayInputStream inputStreamStream = new ByteArrayInputStream(outputStream.toByteArray());
    Batch deserializeBatch = Batch.deserialize(inputStreamStream);

    assertEquals(batch, deserializeBatch);
  }

  private Record buildRecord(long time, int size) {
    List<Object> value = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      value.add("v" + i);
    }

    return new Record(time, value);
  }
}
