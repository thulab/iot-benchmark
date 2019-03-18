package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.workload.Workload;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private DBWrapper dbWrapper;
  private OperationController operationController;
  private Measurement measurement;

  public int getClientThreadId() {
    return clientThreadId;
  }

  public void setClientThreadId(int clientThreadId) {
    this.clientThreadId = clientThreadId;
  }

  private int clientThreadId;

  private Workload workload;

  public Client(int id) {
    clientThreadId = id;
    workload = new Workload(id);
    operationController = new OperationController(id);
  }

  @Override
  public void run() {
    switch (config.BENCHMARK_WORK_MODE) {
      case Constants.MODE_TEST_WITH_DEFAULT_PATH:
        doTestWithDefaultPath();
        break;
      case Constants.MODE_UNBIND_WITH_DEFAULT_PATH:
        doUnbindTestWithDefaultPath();
      default:

    }
  }

  private void doTestWithDefaultPath() {
    for (long loopIndex = 0; loopIndex < config.LOOP; loopIndex++) {
      Operation operation = operationController.getNextOperationType();
      switch (operation) {
        case INGESTION:
          try {
            dbWrapper.insertOneBatch(workload.getOneBatch(), measurement);
          } catch (WorkloadException e) {
            LOGGER.error("Failed to insert one batch data, please check workload parameters.", e);
          }
          break;
        case PRECISE_QUERY:
          dbWrapper.preciseQuery(workload.getPreciseQuery(), measurement);
          break;
        case RANGE_QUERY:
          dbWrapper.rangeQuery(workload.getRangeQuery(), measurement);
          break;
        case VALUE_RANGE_QUERY:
          dbWrapper.valueRangeQuery(workload.getValueRangeQuery(), measurement);
          break;
        case AGG_RANGE_QUERY:
          dbWrapper.aggRangeQuery(workload.getAggRangeQuery(), measurement);
          break;
        case AGG_VALUE_QUERY:
          dbWrapper.aggValueQuery(workload.getAggValueQuery(), measurement);
          break;
        case AGG_RANGE_VALUE_QUERY:
          dbWrapper.aggRangeValueQuery(workload.getAggRangeValueQuery(), measurement);
          break;
        case GROUP_BY_QUERY:
          dbWrapper.groupByQuery(workload.getGroupByQuery(), measurement);
          break;
        case LATEST_POINT_QUERY:
          dbWrapper.latestPointQuery(workload.getLatestPointQuery(), measurement);
          break;
        default:
          LOGGER.error("Unsupported operation type {}", operation);
      }
    }
  }

  private void doUnbindTestWithDefaultPath() {

  }
}
