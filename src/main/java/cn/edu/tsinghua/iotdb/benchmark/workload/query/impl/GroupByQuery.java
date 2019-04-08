package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.query.Query;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;


public class GroupByQuery extends Query {

  private List<DeviceSchema> deviceSchema;
  private String aggFun;
  // also used to be the segment start time
  private long startTimestamp;
  private long endTimestamp;
  private long interval;

  public GroupByQuery(List<DeviceSchema> deviceSchema, String aggFun, long interval, long startTimestamp, long endTimestamp) {
    this.deviceSchema = deviceSchema;
    this.aggFun = aggFun;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.interval = interval;
  }

  public long getInterval() {
    return interval;
  }

  public String getAggFun() {
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
