package cn.edu.tsinghua.iotdb.benchmark.tsdb.rocketmq;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMQ implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQ.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private DefaultMQProducer producer;

  @Override
  public void init() throws TsdbException {
    producer = new DefaultMQProducer("ProducerGroupName", true);
    producer.setNamesrvAddr(config.HOST);
    try {
      producer.start();
    } catch (MQClientException e) {
      throw new TsdbException("start producer failed ", e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {

  }

  @Override
  public void close() throws TsdbException {
    try {
      Thread.sleep(config.INIT_WAIT_TIME);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    producer.shutdown();
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {

  }

  @Override
  public Status insertOneBatch(Batch batch) {
    long st;
    long en;
    String storageGroup = batch.getDeviceSchema().getGroup();
    String device = batch.getDeviceSchema().getDevice();
    try {
      st = System.nanoTime();
      for (Record record : batch.getRecords()) {
        String tag = device + "." + record.getTimestamp();
        Message msg;
        try {
          msg = new Message(storageGroup, tag,
              (record.toString()).getBytes(RemotingHelper.DEFAULT_CHARSET));
          SendResult sendResult = producer.send(msg);
          LOGGER.debug("Send result: {}", sendResult);
        } catch (UnsupportedEncodingException e) {
          LOGGER.error("Message getBytes failed", e);
        }
      }
      en = System.nanoTime();
      return new Status(true, en - st);
    } catch (Exception e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return null;
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    return null;
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return null;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return null;
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return null;
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return null;
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return null;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return null;
  }
}
