package cn.edu.tsinghua.iotdb.benchmark.tsdb;

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

public interface IDatabase {

  void init();

  void cleanup();

  void close();

  void createSchema(Measurement measurement);

  void insertOneBatch(Batch batch, Measurement measurement);

  void preciseQuery(PreciseQuery preciseQuery, Measurement measurement);

  void rangeQuery(RangeQuery rangeQuery, Measurement measurement);

  void valueRangeQuery(ValueRangeQuery valueRangeQuery, Measurement measurement);

  void aggRangeQuery(AggRangeQuery aggRangeQuery, Measurement measurement);

  void aggValueQuery(AggValueQuery aggValueQuery, Measurement measurement);

  void aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery, Measurement measurement);

  void groupByQuery(GroupByQuery groupByQuery, Measurement measurement);

  void latestPointQuery(LatestPointQuery latestPointQuery, Measurement measurement);

}
