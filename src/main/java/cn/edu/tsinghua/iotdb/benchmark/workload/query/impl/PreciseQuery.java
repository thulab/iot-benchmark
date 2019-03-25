package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.query.Query;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;

public class PreciseQuery extends Query {

  private List<DeviceSchema> deviceSchema;
  private long timestamp;

  public List<DeviceSchema> getDeviceSchema() {
    return deviceSchema;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public PreciseQuery(List<DeviceSchema> deviceSchema, long timestamp) {
    this.deviceSchema = deviceSchema;
    this.timestamp = timestamp;
  }
}
