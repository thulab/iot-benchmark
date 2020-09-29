package cn.edu.tsinghua.iotdb.benchmark.kafka;

import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBFactory;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class BatchConsumeThread implements Runnable {

  private final KafkaStream<String, Batch> stream;
  private IDatabase session;

  public BatchConsumeThread(KafkaStream<String, Batch> stream, String host, String port,
      String user, String password) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    this.stream = stream;
    /*
     * Establish session connection of IoTDB
     */
    session = (IDatabase) Class.forName("cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDB").newInstance();
  }

  @Override
  public void run() {
    for (MessageAndMetadata<String, Batch> consumerIterator : stream) {
      session.insertOneBatch(consumerIterator.message());
    }
  }
}
