package cn.edu.tsinghua.iotdb.benchmark.kafka;


import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class BatchSerializer implements Serializer<Batch> {

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public byte[] serialize(String s, Batch batch) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      batch.serialize(outputStream);
      return outputStream.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new byte[0];
  }

  @Override
  public void close() {

  }


}
