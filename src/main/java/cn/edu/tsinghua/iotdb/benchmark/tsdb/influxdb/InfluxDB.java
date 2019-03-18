package cn.edu.tsinghua.iotdb.benchmark.tsdb.influxdb;

import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;

public class InfluxDB implements IDatabase {

  @Override
  public void init() {

  }

  @Override
  public void cleanup() {

  }

  @Override
  public void close() {

  }

  @Override
  public void registerSchema(Measurement measurement) {

  }

  @Override
  public void insertOneBatch(Batch batch, Measurement measurement) {

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
}
