package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
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


  public DBWrapper() {
    DBFactory dbFactory = new DBFactory();
    try {
      db = dbFactory.getDatabase();
    } catch (Exception e) {
      LOGGER.error("Failed to get database because", e);
    }
  }

  @Override
  public void insertOneBatch(Batch batch, Measurement measurement) {
    long st;
    long en;
    double timeInMillis;
    st = System.nanoTime();
    db.insertOneBatch(batch, measurement);
    en = System.nanoTime();
    timeInMillis = (en - st ) / NANO_TO_MILLIS;
    measurement.addOperationLatency(Operation.INGESTION, timeInMillis);
  }

  @Override
  public void preciseQuery(PreciseQuery preciseQuery, Measurement measurement) {

  }

  @Override
  public void rangeQuery(RangeQuery rangeQuery, Measurement measurement) {

  }

  @Override
  public void valueRangeQuery(ValueRangeQuery valueRangeQuery, Measurement measurement) {

  }

  @Override
  public void aggRangeQuery(AggRangeQuery aggRangeQuery, Measurement measurement) {

  }

  @Override
  public void aggValueQuery(AggValueQuery aggValueQuery, Measurement measurement) {

  }

  @Override
  public void aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery, Measurement measurement) {

  }

  @Override
  public void groupByQuery(GroupByQuery groupByQuery, Measurement measurement) {

  }

  @Override
  public void latestPointQuery(LatestPointQuery latestPointQuery, Measurement measurement) {

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
      createSchemaTimeInSecond = (st - en) / NANO_TO_SECOND;
      measurement.setCreateSchemaTime(createSchemaTimeInSecond);
    } catch (Exception e) {
      LOGGER.error("Fail to create schema because {}", e);
    }
    measurement.setCreateSchemaTime(createSchemaTimeInSecond);
  }


}
