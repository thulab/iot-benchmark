package cn.edu.tsinghua.iotdb.benchmark.workload;

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

//TODO not used yet
interface IWorkload {

  Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException;

  PreciseQuery getPreciseQuery() throws WorkloadException;

  RangeQuery getRangeQuery() throws WorkloadException;

  ValueRangeQuery getValueRangeQuery();

  AggRangeQuery getAggRangeQuery();

  AggValueQuery getAggValueQuery();

  AggRangeValueQuery getAggRangeValueQuery();

  GroupByQuery getGroupByQuery();

  LatestPointQuery getLatestPointQuery();

}
