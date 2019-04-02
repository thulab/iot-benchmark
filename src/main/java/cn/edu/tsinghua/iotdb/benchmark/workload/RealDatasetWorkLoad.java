package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.ReddReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.TDriveReader;
import java.util.List;

public class RealDatasetWorkLoad implements IWorkload{

  private BasicReader reader;

  public RealDatasetWorkLoad(List<String> files, Config config) {
    switch (config.DATA_SET) {
      case "TDRIVE":
        reader = new TDriveReader(config, files);
        break;
      case "REDD":
        reader = new ReddReader(config, files);
        break;
      default:
        throw new RuntimeException(config.DATA_SET + " not supported");
    }
  }

  @Override
  public Batch getOneBatch() {
    if(reader.hasNextBatch()) {
      return reader.nextBatch();
    } else {
      return null;
    }
  }

  @Override
  public PreciseQuery getPreciseQuery() throws WorkloadException {
    return null;
  }

  @Override
  public RangeQuery getRangeQuery() throws WorkloadException {
    return null;
  }

  @Override
  public ValueRangeQuery getValueRangeQuery() {
    return null;
  }

  @Override
  public AggRangeQuery getAggRangeQuery() {
    return null;
  }

  @Override
  public AggValueQuery getAggValueQuery() {
    return null;
  }

  @Override
  public AggRangeValueQuery getAggRangeValueQuery() {
    return null;
  }

  @Override
  public GroupByQuery getGroupByQuery() {
    return null;
  }

  @Override
  public LatestPointQuery getLatestPointQuery() {
    return null;
  }
}
