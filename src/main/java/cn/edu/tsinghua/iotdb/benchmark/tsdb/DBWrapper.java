package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBWrapper implements IDBWrapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(IDatabase.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private IDatabase db;
  private static final double NANO_TO_SECOND = 1000000000.0d;
  private static final double NANO_TO_MILLIS = 1000000.0d;
  private Measurement measurement;


  public DBWrapper(Measurement measurement) {
    DBFactory dbFactory = new DBFactory();
    try {
      db = dbFactory.getDatabase();
    } catch (Exception e) {
      LOGGER.error("Failed to get database because", e);
    }
    this.measurement = measurement;
  }

  @Override
  public void insertOneBatch(Batch batch) {
    Status status;
    Operation operation = Operation.INGESTION;
    try {
      status = db.insertOneBatch(batch);
      if (status.isOk()) {
        double timeInMillis = status.getCostTime() / NANO_TO_MILLIS;
        measureOperation(status, operation, batch.pointNum());
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        String currentThread = Thread.currentThread().getName();
        LOGGER.info("{} insert one batch latency ,{}, ms", currentThread, formatTimeInMillis);
      } else {
        measurement.addFailOperationNum(operation);
        measurement.addFailPointNum(operation, batch.pointNum());
      }
    } catch (Exception e) {
      measurement.addFailOperationNum(operation);
      measurement.addFailPointNum(operation, batch.pointNum());
      LOGGER.error("Failed to insert one batch because unexpected exception: ", e);
    }
  }

  @Override
  public void preciseQuery(PreciseQuery preciseQuery) {
    Status status;
    Operation operation = Operation.PRECISE_QUERY;
    try {
      status = db.preciseQuery(preciseQuery);
      if(status.isOk()){
        double timeInMillis = status.getCostTime() / NANO_TO_MILLIS;
        measureOperation(status, operation, status.getQueryResultPointNum());
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        String currentThread = Thread.currentThread().getName();
        LOGGER.info("{} complete precise query with latency ,{}, ms", currentThread, formatTimeInMillis);
      } else {
        measurement.addFailOperationNum(operation);
        // currently we do not have expected result point number
        measurement.addOkPointNum(operation, status.getQueryResultPointNum());
      }
    } catch (Exception e) {
      measurement.addFailOperationNum(operation);
      // currently we do not have expected result point number
      LOGGER.error("Failed to do precise query because unexpected exception: ", e);
    }
  }

  @Override
  public void rangeQuery(RangeQuery rangeQuery) {
    Status status;
    Operation operation = Operation.RANGE_QUERY;
    try {
      status = db.rangeQuery(rangeQuery);
      if(status.isOk()){
        measureOperation(status, operation, status.getQueryResultPointNum());
        double timeInMillis = status.getCostTime() / NANO_TO_MILLIS;
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        String currentThread = Thread.currentThread().getName();
        LOGGER.info("{} complete range query with latency ,{}, ms", currentThread, formatTimeInMillis);
      } else {
        measurement.addFailOperationNum(operation);
        // currently we do not have expected result point number
        measurement.addOkPointNum(operation, status.getQueryResultPointNum());
      }
    } catch (Exception e) {
      measurement.addFailOperationNum(operation);
      // currently we do not have expected result point number
      LOGGER.error("Failed to do range query because unexpected exception: ", e);
    }
  }

  private void measureOperation(Status status, Operation operation, int okPointNum){
    measurement.addOperationLatency(operation, status.getCostTime() / NANO_TO_MILLIS);
    measurement.addOkOperationNum(operation);
    measurement.addOkPointNum(operation, okPointNum);
  }

  @Override
  public void valueRangeQuery(ValueRangeQuery valueRangeQuery) {

  }

  @Override
  public void aggRangeQuery(AggRangeQuery aggRangeQuery) {

  }

  @Override
  public void aggValueQuery(AggValueQuery aggValueQuery) {

  }

  @Override
  public void aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {

  }

  @Override
  public void groupByQuery(GroupByQuery groupByQuery) {

  }

  @Override
  public void latestPointQuery(LatestPointQuery latestPointQuery) {

  }

  @Override
  public void init() {
    db.init();
  }

  @Override
  public void cleanup() {
    db.cleanup();
  }

  @Override
  public void close() {
    db.close();
  }

  @Override
  public void registerSchema(Measurement measurement) {
    double createSchemaTimeInSecond = 0;
    long en = 0;
    long st = 0;
    db.init();
    LOGGER.info("Registering schema...");
    try {
      if (config.CREATE_SCHEMA) {
        st = System.nanoTime();
        db.registerSchema(measurement);
        en = System.nanoTime();
      }
      db.close();
      createSchemaTimeInSecond = (en - st) / NANO_TO_SECOND;
      measurement.setCreateSchemaTime(createSchemaTimeInSecond);
    } catch (Exception e) {
      LOGGER.error("Fail to create schema because {}", e);
    }
    measurement.setCreateSchemaTime(createSchemaTimeInSecond);
  }


}
