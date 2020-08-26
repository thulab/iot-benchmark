package cn.edu.tsinghua.iotdb.benchmark.kafka;

import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.IoTDBSession;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class BatchConsumeThread implements Runnable {

  private final KafkaStream<String, Batch> stream;
  private IoTDBSession session;

  public BatchConsumeThread(KafkaStream<String, Batch> stream, String host, String port,
      String user, String password) {
    this.stream = stream;
    /*
     * Establish session connection of IoTDB
     */
    session = new IoTDBSession(host, port, user, password);
  }

  @Override
  public void run() {
    for (MessageAndMetadata<String, Batch> consumerIterator : stream) {
      session.insertOneBatch(consumerIterator.message());
    }
  }
}
