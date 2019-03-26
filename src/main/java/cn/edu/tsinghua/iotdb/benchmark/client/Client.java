package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.workload.SingletonWorkload;
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
  private final SingletonWorkload singletonWorkload;
  private long insertLoopIndex;
  private DataSchema dataSchema = DataSchema.getInstance();

  public Client(int id, CountDownLatch countDownLatch) {
    this.countDownLatch = countDownLatch;
    clientThreadId = id;
    workload = new Workload(id);
    singletonWorkload = SingletonWorkload.getInstance();
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
      try {
        doTestWithDefaultPath();
      } catch (Exception e) {
        LOGGER.error("Unexpected error: ", e);
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
          if (config.IS_CLIENT_BIND) {
            try {
              List<DeviceSchema> schema = dataSchema.getClientBindSchema().get(clientThreadId);
              for (DeviceSchema deviceSchema : schema) {
                dbWrapper.insertOneBatch(workload.getOneBatch(deviceSchema, insertLoopIndex));
              }
            } catch (Exception e) {
              LOGGER.error("Failed to insert one batch data because ", e);
            }
            insertLoopIndex++;
          } else {
            try {
              dbWrapper.insertOneBatch(singletonWorkload.getOneBatch());
            } catch (Exception e) {
              LOGGER.error("Failed to insert one batch data because ", e);
            }
          }
          break;
        case PRECISE_QUERY:
          try {
            dbWrapper.preciseQuery(workload.getPreciseQuery());
          } catch (Exception e) {
            LOGGER.error("Failed to do precise query because ", e);
          }
          break;
        case RANGE_QUERY:
          try {
            dbWrapper.rangeQuery(workload.getRangeQuery());
          } catch (Exception e) {
            LOGGER.error("Failed to do range query because ", e);
          }
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
      String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / config.LOOP);
      LOGGER.info("{} {}% workload is done.", Thread.currentThread().getName(), percent);
    }
  }

}
