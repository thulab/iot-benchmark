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
    try {
      status = db.insertOneBatch(batch);
      if (status.isOk()) {
        double timeInMillis = status.getCostTime() / NANO_TO_MILLIS;
        measurement.addOperationLatency(Operation.INGESTION, timeInMillis);
        measurement.addOkOperationNum(Operation.INGESTION);
        measurement.addOkPointNum(Operation.INGESTION, batch.pointNum());
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        String currentThread = Thread.currentThread().getName();
        LOGGER.info("{} insert one batch latency ,{}, ms", currentThread, formatTimeInMillis);
      } else {
        measurement.addFailOperationNum(Operation.INGESTION);
        measurement.addFailPointNum(Operation.INGESTION, batch.pointNum());
      }
    } catch (Exception e) {
      measurement.addFailOperationNum(Operation.INGESTION);
      measurement.addFailPointNum(Operation.INGESTION, batch.pointNum());
      LOGGER.error("Unknown Exception occurred Failed to insert one batch because ", e);
    }
  }

  @Override
  public void preciseQuery(PreciseQuery preciseQuery) {

  }

  @Override
  public void rangeQuery(RangeQuery rangeQuery) {

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
