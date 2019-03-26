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

public interface IDBWrapper {

  void init();

  void cleanup();

  void close();

  void closeSingleDBInstance();

  void registerSchema(Measurement measurement);

  void insertOneBatch(Batch batch);

  void preciseQuery(PreciseQuery preciseQuery);

  void rangeQuery(RangeQuery rangeQuery);

  void valueRangeQuery(ValueRangeQuery valueRangeQuery);

  void aggRangeQuery(AggRangeQuery aggRangeQuery);

  void aggValueQuery(AggValueQuery aggValueQuery);

  void aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery);

  void groupByQuery(GroupByQuery groupByQuery);

  void latestPointQuery(LatestPointQuery latestPointQuery);
}
