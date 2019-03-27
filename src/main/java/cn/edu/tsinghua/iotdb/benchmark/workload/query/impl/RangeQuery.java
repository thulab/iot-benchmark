package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.query.Query;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;

public class RangeQuery extends Query {
  private List<DeviceSchema> deviceSchema;
  private long startTimestamp;
  private long endTimestamp;

  public long getStartTimestamp() {
    return startTimestamp;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }

  public List<DeviceSchema> getDeviceSchema() {
    return deviceSchema;
  }

  public RangeQuery(List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp) {
    this.deviceSchema = deviceSchema;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
  }
}
