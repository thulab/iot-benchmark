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

public interface IWorkload {

    Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException;

    Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex,int colIndex) throws WorkloadException;

    PreciseQuery getPreciseQuery() throws WorkloadException;

    RangeQuery getRangeQuery() throws WorkloadException;

    ValueRangeQuery getValueRangeQuery() throws WorkloadException;

    AggRangeQuery getAggRangeQuery() throws WorkloadException;

    AggValueQuery getAggValueQuery() throws WorkloadException;

    AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException;

    GroupByQuery getGroupByQuery() throws WorkloadException;

    LatestPointQuery getLatestPointQuery() throws WorkloadException;

}
