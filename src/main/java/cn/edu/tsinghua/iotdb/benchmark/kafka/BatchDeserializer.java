package cn.edu.tsinghua.iotdb.benchmark.kafka;

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import kafka.serializer.Decoder;

public class BatchDeserializer implements Decoder<Batch> {

  @Override
  public Batch fromBytes(byte[] bytes) {
    try {
      return Batch.deserialize(new ByteArrayInputStream(bytes));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }
}
