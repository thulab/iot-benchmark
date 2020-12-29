package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBWrapper implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBWrapper.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private IDatabase db;
  private static final double NANO_TO_SECOND = 1000000000.0d;
  private static final double NANO_TO_MILLIS = 1000000.0d;
  private Measurement measurement;
  private static final String ERROR_LOG = "Failed to do {} because unexpected exception: ";
  private ITestDataPersistence recorder;

  public DBWrapper(Measurement measurement) {
    DBFactory dbFactory = new DBFactory();
    try {
      db = dbFactory.getDatabase();
    } catch (Exception e) {
      LOGGER.error("Failed to get database because", e);
    }
    this.measurement = measurement;
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    recorder = persistenceFactory.getPersistence();
  }

  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    Status status = null;
    Operation operation = Operation.INGESTION;
    try {

      long st = System.nanoTime();
      status = db.insertOneBatch(batch);
      long en = System.nanoTime();
      status.setTimeCost(en - st);

      if (status.isOk()) {
        measureOkOperation(status, operation, batch.pointNum());
        if (!config.isIS_QUIET_MODE()) {
          double timeInMillis = status.getTimeCost() / NANO_TO_MILLIS;
          String formatTimeInMillis = String.format("%.2f", timeInMillis);
          double throughput = batch.pointNum() * 1000 / timeInMillis;
          LOGGER.info("{} insert one batch latency (device: {}, sg: {}) ,{}, ms, throughput ,{}, points/s",
              Thread.currentThread().getName(), batch.getDeviceSchema().getDevice(),
              batch.getDeviceSchema().getGroup(), formatTimeInMillis, throughput);
        }
      } else {
        measurement.addFailOperationNum(operation);
        measurement.addFailPointNum(operation, batch.pointNum());
        recorder.saveOperationResult(operation.getName(), 0, batch.pointNum(), 0,
            status.getException().toString());
        LOGGER.error("Insert batch failed because", status.getException());
      }
    } catch (DBConnectException ex) {
      throw ex;
    } catch (Exception e) {
      measurement.addFailOperationNum(operation);
      measurement.addFailPointNum(operation, batch.pointNum());
      recorder.saveOperationResult(operation.getName(), 0, batch.pointNum(), 0, e.toString());
      LOGGER.error("Failed to insert one batch because unexpected exception: ", e);
    }
    return status;
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    Status status = null;
    Operation operation = Operation.PRECISE_QUERY;
    try {
      long st = System.nanoTime();
      status = db.preciseQuery(preciseQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }


  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    Status status = null;
    Operation operation = Operation.RANGE_QUERY;
    try {
      long st = System.nanoTime();
      status = db.rangeQuery(rangeQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }


  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    Status status = null;
    Operation operation = Operation.VALUE_RANGE_QUERY;
    try {
      long st = System.nanoTime();
      status = db.valueRangeQuery(valueRangeQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    Status status = null;
    Operation operation = Operation.AGG_RANGE_QUERY;
    try {
      long st = System.nanoTime();
      status = db.aggRangeQuery(aggRangeQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    Status status = null;
    Operation operation = Operation.AGG_VALUE_QUERY;
    try {
      long st = System.nanoTime();
      status = db.aggValueQuery(aggValueQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    Status status = null;
    Operation operation = Operation.AGG_RANGE_VALUE_QUERY;
    try {
      long st = System.nanoTime();
      status = db.aggRangeValueQuery(aggRangeValueQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    Status status = null;
    Operation operation = Operation.GROUP_BY_QUERY;
    try {
      long st = System.nanoTime();
      status = db.groupByQuery(groupByQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    Status status = null;
    Operation operation = Operation.LATEST_POINT_QUERY;
    try {
      long st = System.nanoTime();
      status = db.latestPointQuery(latestPointQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    Status status = null;
    Operation operation = Operation.RANGE_QUERY_ORDER_BY_TIME_DESC;
    try {
      rangeQuery.setDesc(true);
      long st = System.nanoTime();
      status = db.rangeQueryOrderByDesc(rangeQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    Status status = null;
    Operation operation = Operation.VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC;
    try {
      valueRangeQuery.setDesc(true);
      long st = System.nanoTime();
      status = db.valueRangeQueryOrderByDesc(valueRangeQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
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
    if (recorder != null) {
      recorder.close();
    }
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    double createSchemaTimeInSecond;
    long en = 0;
    long st = 0;
    LOGGER.info("Registering schema...");
    try {
      if (config.isCREATE_SCHEMA()) {
        st = System.nanoTime();
        db.registerSchema(schemaList);
        en = System.nanoTime();
      }
      createSchemaTimeInSecond = (en - st) / NANO_TO_SECOND;
      measurement.setCreateSchemaTime(createSchemaTimeInSecond);
    } catch (Exception e) {
      measurement.setCreateSchemaTime(0);
      throw new TsdbException(e);
    }
  }

  private void handleUnexpectedQueryException(Operation operation, Exception e) {
    measurement.addFailOperationNum(operation);
    // currently we do not have expected result point number for query
    LOGGER.error(ERROR_LOG, operation, e);
    recorder.saveOperationResult(operation.getName(), 0, 0, 0, e.toString());
  }

  private void measureOkOperation(Status status, Operation operation, int okPointNum) {
    double latencyInMillis = status.getTimeCost() / NANO_TO_MILLIS;
    if(latencyInMillis < 0) {
      LOGGER.warn("Operation {} may have exception since the latency is negative, set it to zero",
          operation.getName());
      latencyInMillis = 0;
    }
    measurement.addOperationLatency(operation, latencyInMillis);
    measurement.addOkOperationNum(operation);
    measurement.addOkPointNum(operation, okPointNum);
    recorder.saveOperationResult(operation.getName(), okPointNum, 0, latencyInMillis, "");
  }

  private void handleQueryOperation(Status status, Operation operation) {
    if (status.isOk()) {
      measureOkOperation(status, operation, status.getQueryResultPointNum());
      if(!config.isIS_QUIET_MODE()) {
        double timeInMillis = status.getTimeCost() / NANO_TO_MILLIS;
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        String currentThread = Thread.currentThread().getName();
        LOGGER
            .info("{} complete {} with latency ,{}, ms ,{}, result points", currentThread, operation,
                formatTimeInMillis, status.getQueryResultPointNum());
      }
    } else {
      LOGGER.error("Execution fail: {}", status.getErrorMessage(), status.getException());
      measurement.addFailOperationNum(operation);
      // currently we do not have expected result point number for query
      recorder
          .saveOperationResult(operation.getName(), 0, 0, 0, status.getException().toString());
    }
  }

}
