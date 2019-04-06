package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.query.Query;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;


public class GroupByQuery extends Query {

  private List<DeviceSchema> deviceSchema;
  private AggFunction aggFun;
  // also used to be the segment start time
  private long startTimestamp;
  private long endTimestamp;
  private long interval;

  public GroupByQuery(List<DeviceSchema> deviceSchema, AggFunction aggFun, long startTimestamp, long endTimestamp, long interval) {
    this.deviceSchema = deviceSchema;
    this.aggFun = aggFun;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.interval = interval;
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
