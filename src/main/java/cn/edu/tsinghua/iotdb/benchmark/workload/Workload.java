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

public class Workload {


  public Batch getOrderedBatch() {
    return null;
  }

  public Batch getDistOutOfOrderBatch() {
    return null;
  }

  public Batch getLocalOutOfOrderBatch(){
    return null;
  }

  public Batch getGlobalOutOfOrderBatch(){
    return null;
  }

  public PreciseQuery getPreciseQuery(){
    return null;
  }

  public RangeQuery getRangeQuery(){
    return null;
  }

  public ValueRangeQuery getValueRangeQuery(){
    return null;
  }

  public AggRangeQuery getAggRangeQuery(){
    return null;
  }

  public AggValueQuery getAggValueQuery(){
    return null;
  }

  public AggRangeValueQuery getAggRangeValueQuery(){
    return null;
  }

  public GroupByQuery getGroupByQuery(){
    return null;
  }

  public LatestPointQuery getLatestPointQuery(){
    return null;
  }


}
