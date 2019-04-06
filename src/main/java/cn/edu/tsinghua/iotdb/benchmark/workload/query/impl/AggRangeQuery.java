package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.query.Query;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;

public class AggRangeQuery extends Query {

  private List<DeviceSchema> deviceSchema;
  private AggFunction aggFun;
  private long startTimestamp;
  private long endTimestamp;

  public AggRangeQuery(List<DeviceSchema> deviceSchema, AggFunction aggFun, long startTimestamp, long endTimestamp) {
    this.deviceSchema = deviceSchema;
    this.aggFun = aggFun;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
  }

  public AggFunction getAggFun() {
    return aggFun;
  }

  public long getStartTimestamp() {
    return startTimestamp;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }

  public List<DeviceSchema> getDeviceSchema() {
    return deviceSchema;
  }

}
