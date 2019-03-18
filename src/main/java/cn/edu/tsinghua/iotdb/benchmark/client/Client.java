package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private DBWrapper dbWrapper;
  private OperationController operationController;

  public int getClientThreadId() {
    return clientThreadId;
  }

  public void setClientThreadId(int clientThreadId) {
    this.clientThreadId = clientThreadId;
  }

  private int clientThreadId;

  public Client(int id) {
    clientThreadId = id;
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
          dbWrapper.insertOneBatch();
          break;
        case PRECISE_QUERY:
          dbWrapper.preciseQuery();
          break;
        case RANGE_QUERY:
          dbWrapper.rangeQuery();
          break;
        case VALUE_RANGE_QUERY:
          dbWrapper.valueRangeQuery();
          break;
        case AGG_RANGE_QUERY:
          dbWrapper.aggRangeQuery();
          break;
        case AGG_VALUE_QUERY:
          dbWrapper.aggValueQuery();
          break;
        case AGG_RANGE_VALUE_QUERY:
          dbWrapper.aggRangeValueQuery();
          break;
        case GROUP_BY_QUERY:
          dbWrapper.groupByQuery();
          break;
        case LATEST_POINT_QUERY:
          dbWrapper.latestPointQuery();
          break;
        default:
          LOGGER.error("Unsupported operation type {}", operation);
      }
    }
  }

  private void doUnbindTestWithDefaultPath() {

  }
}
