package cn.edu.tsinghua.iotdb.benchmark.serialize;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BatchSerializeTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testSerialize() throws Exception {
    String group = "g1";
    String device = "d1";
    List<String> sensors = new ArrayList<>();
    sensors.add("s1");
    sensors.add("s2");
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
