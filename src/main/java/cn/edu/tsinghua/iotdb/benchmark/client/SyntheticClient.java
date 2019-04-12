package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.workload.SingletonWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.SyntheticWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyntheticClient extends Client implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyntheticClient.class);

  private OperationController operationController;
  private SyntheticWorkload syntheticWorkload;
  private final SingletonWorkload singletonWorkload;
  private long insertLoopIndex;
  private DataSchema dataSchema = DataSchema.getInstance();


  public SyntheticClient(int id, CountDownLatch countDownLatch) {
    super(id, countDownLatch);
    syntheticWorkload = new SyntheticWorkload(id);
    singletonWorkload = SingletonWorkload.getInstance();
    operationController = new OperationController(id);
    insertLoopIndex = 0;
  }

  void doTest() {
    for (long loopIndex = 0; loopIndex < config.LOOP; loopIndex++) {
      Operation operation = operationController.getNextOperationType();
      switch (operation) {
        case INGESTION:
          if (config.IS_CLIENT_BIND) {
            try {
              List<DeviceSchema> schema = dataSchema.getClientBindSchema().get(clientThreadId);
              for (DeviceSchema deviceSchema : schema) {
                dbWrapper.insertOneBatch(syntheticWorkload.getOneBatch(deviceSchema, insertLoopIndex));
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
            dbWrapper.preciseQuery(syntheticWorkload.getPreciseQuery());
          } catch (Exception e) {
            LOGGER.error("Failed to do precise query because ", e);
          }
          break;
        case RANGE_QUERY:
          try {
            dbWrapper.rangeQuery(syntheticWorkload.getRangeQuery());
          } catch (Exception e) {
            LOGGER.error("Failed to do range query because ", e);
          }
          break;
        case VALUE_RANGE_QUERY:
          try {
            dbWrapper.valueRangeQuery(syntheticWorkload.getValueRangeQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do range query with value filter because ", e);
          }
          break;
        case AGG_RANGE_QUERY:
          try {
            dbWrapper.aggRangeQuery(syntheticWorkload.getAggRangeQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do aggregation range query because ", e);
          }
          break;
        case AGG_VALUE_QUERY:
          try {
            dbWrapper.aggValueQuery(syntheticWorkload.getAggValueQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do aggregation query with value filter because ", e);
          }
          break;
        case AGG_RANGE_VALUE_QUERY:
          try {
            dbWrapper.aggRangeValueQuery(syntheticWorkload.getAggRangeValueQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do aggregation range query with value filter because ", e);
          }
          break;
        case GROUP_BY_QUERY:
          try {
            dbWrapper.groupByQuery(syntheticWorkload.getGroupByQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do group by query because ", e);
          }
          break;
        case LATEST_POINT_QUERY:
          try {
            dbWrapper.latestPointQuery(syntheticWorkload.getLatestPointQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do latest point query because ", e);
          }
          break;
        default:
          LOGGER.error("Unsupported operation type {}", operation);
      }
      String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / config.LOOP);
      LOGGER.info("{} {}% syntheticWorkload is done.", Thread.currentThread().getName(), percent);
    }
  }

}
