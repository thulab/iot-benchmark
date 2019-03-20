package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.workload.Workload;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private DBWrapper dbWrapper;
  private OperationController operationController;
  private Measurement measurement;
  private CountDownLatch countDownLatch;
  private int clientThreadId;
  private Workload workload;
  private long insertLoopIndex;
  private DataSchema dataSchema = DataSchema.getInstance();

  public Client(int id, CountDownLatch countDownLatch) {
    this.countDownLatch = countDownLatch;
    clientThreadId = id;
    workload = new Workload(id);
    operationController = new OperationController(id);
    measurement = new Measurement();
    dbWrapper = new DBWrapper(measurement);
    insertLoopIndex = 0;
  }

  public Measurement getMeasurement() {
    return measurement;
  }

  @Override
  public void run() {
    try {
      switch (config.BENCHMARK_WORK_MODE) {
        case Constants.MODE_TEST_WITH_DEFAULT_PATH:
          doTestWithDefaultPath();
          break;
        case Constants.MODE_UNBIND_WITH_DEFAULT_PATH:
          doUnbindTestWithDefaultPath();
        default:
      }
      dbWrapper.close();
    } finally {
      countDownLatch.countDown();
    }
  }

  private void doTestWithDefaultPath() {
    for (long loopIndex = 0; loopIndex < config.LOOP; loopIndex++) {
      Operation operation = operationController.getNextOperationType();
      switch (operation) {
        case INGESTION:
          try {
            List<DeviceSchema> schema = dataSchema.getClientBindSchema().get(clientThreadId);
            for (DeviceSchema deviceSchema: schema) {
              dbWrapper.insertOneBatch(workload.getOneBatch(deviceSchema, insertLoopIndex));
            }
          } catch (Exception e) {
            LOGGER.error("Failed to insert one batch data, please check workload parameters.", e);
          }
          insertLoopIndex++;
          break;
        case PRECISE_QUERY:
          dbWrapper.preciseQuery(workload.getPreciseQuery());
          break;
        case RANGE_QUERY:
          dbWrapper.rangeQuery(workload.getRangeQuery());
          break;
        case VALUE_RANGE_QUERY:
          dbWrapper.valueRangeQuery(workload.getValueRangeQuery());
          break;
        case AGG_RANGE_QUERY:
          dbWrapper.aggRangeQuery(workload.getAggRangeQuery());
          break;
        case AGG_VALUE_QUERY:
          dbWrapper.aggValueQuery(workload.getAggValueQuery());
          break;
        case AGG_RANGE_VALUE_QUERY:
          dbWrapper.aggRangeValueQuery(workload.getAggRangeValueQuery());
          break;
        case GROUP_BY_QUERY:
          dbWrapper.groupByQuery(workload.getGroupByQuery());
          break;
        case LATEST_POINT_QUERY:
          dbWrapper.latestPointQuery(workload.getLatestPointQuery());
          break;
        default:
          LOGGER.error("Unsupported operation type {}", operation);
      }
    }
  }

  private void doUnbindTestWithDefaultPath() {

  }
}
