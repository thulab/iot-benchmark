package cn.edu.tsinghua.iotdb.benchmark.tsdb;

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
import java.io.IOException;
import java.sql.SQLException;

public interface IDatabase {

  void init();

  void cleanup();

  void close();

  void closeSingleDBInstance() throws IOException;

  void registerSchema(Measurement measurement);

  Status insertOneBatch(Batch batch);

  Status preciseQuery(PreciseQuery preciseQuery);

  Status rangeQuery(RangeQuery rangeQuery);

  Status valueRangeQuery(ValueRangeQuery valueRangeQuery);

  Status aggRangeQuery(AggRangeQuery aggRangeQuery);

  Status aggValueQuery(AggValueQuery aggValueQuery);

  Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery);

  Status groupByQuery(GroupByQuery groupByQuery);

  Status latestPointQuery(LatestPointQuery latestPointQuery);

}
