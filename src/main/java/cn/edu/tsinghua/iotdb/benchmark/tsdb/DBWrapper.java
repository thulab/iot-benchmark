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

public class DBWrapper implements IDatabase {

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
  public Status insertOneBatch(Batch batch) {
    Status status = null;
    Operation operation = Operation.INGESTION;
    try {
      status = db.insertOneBatch(batch);
      if (status.isOk()) {
        double timeInMillis = status.getCostTime() / NANO_TO_MILLIS;
        measureOperation(status, operation, batch.pointNum());
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        String currentThread = Thread.currentThread().getName();
        double throughput = batch.pointNum() * 1000 / timeInMillis;
        LOGGER.info("{} insert one batch latency ,{}, ms, throughput ,{}, points/s", currentThread,
            formatTimeInMillis, throughput);
      } else {
        measurement.addFailOperationNum(operation);
        measurement.addFailPointNum(operation, batch.pointNum());
      }
    } catch (Exception e) {
      measurement.addFailOperationNum(operation);
      measurement.addFailPointNum(operation, batch.pointNum());
      LOGGER.error("Failed to insert one batch because unexpected exception: ", e);
    }
    return status;
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    Status status = null;
    Operation operation = Operation.PRECISE_QUERY;
    try {
      status = db.preciseQuery(preciseQuery);
      if (status.isOk()) {
        double timeInMillis = status.getCostTime() / NANO_TO_MILLIS;
        measureOperation(status, operation, status.getQueryResultPointNum());
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        String currentThread = Thread.currentThread().getName();
        LOGGER
            .info("{} complete range query with latency ,{}, ms ,{}, result points", currentThread,
                formatTimeInMillis, status.getQueryResultPointNum());
      } else {
        LOGGER.error("Execution fail: {}", status.getErrorMessage(), status.getException());
        measurement.addFailOperationNum(operation);
        // currently we do not have expected result point number
        measurement.addOkPointNum(operation, status.getQueryResultPointNum());
      }
    } catch (Exception e) {
      measurement.addFailOperationNum(operation);
      // currently we do not have expected result point number
      LOGGER.error("Failed to do precise query because unexpected exception: ", e);
    }
    return status;
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    Status status = null;
    Operation operation = Operation.RANGE_QUERY;
    try {
      status = db.rangeQuery(rangeQuery);
      if (status.isOk()) {
        measureOperation(status, operation, status.getQueryResultPointNum());
        double timeInMillis = status.getCostTime() / NANO_TO_MILLIS;
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        String currentThread = Thread.currentThread().getName();
        LOGGER
            .info("{} complete range query with latency ,{}, ms ,{}, result points", currentThread,
                formatTimeInMillis, status.getQueryResultPointNum());
      } else {
        LOGGER.error("Execution fail: {}", status.getErrorMessage(), status.getException());
        measurement.addFailOperationNum(operation);
        // currently we do not have expected result point number
        measurement.addOkPointNum(operation, status.getQueryResultPointNum());
      }
    } catch (Exception e) {
      measurement.addFailOperationNum(operation);
      // currently we do not have expected result point number
      LOGGER.error("Failed to do range query because unexpected exception: ", e);
    }
    return status;
  }

  private void measureOperation(Status status, Operation operation, int okPointNum) {
    measurement.addOperationLatency(operation, status.getCostTime() / NANO_TO_MILLIS);
    measurement.addOkOperationNum(operation);
    measurement.addOkPointNum(operation, okPointNum);
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

  public void init() throws TsdbException {
    db.init();
  }

  @Override
  public void cleanup() throws TsdbException {
    db.cleanup();
  }

  @Override
  public void close() throws TsdbException {
    db.close();
  }

  @Override
  public void registerSchema(Measurement measurement) throws TsdbException {
    double createSchemaTimeInSecond;
    long en = 0;
    long st = 0;
    LOGGER.info("Registering schema...");
    try {
      if (config.CREATE_SCHEMA) {
        st = System.nanoTime();
        db.registerSchema(measurement);
        en = System.nanoTime();
      }
      createSchemaTimeInSecond = (en - st) / NANO_TO_SECOND;
      measurement.setCreateSchemaTime(createSchemaTimeInSecond);
    } catch (Exception e) {
      measurement.setCreateSchemaTime(0);
      throw new TsdbException(e);
    }
  }

}
