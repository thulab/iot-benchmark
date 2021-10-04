package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;

public class DeviceQuery extends Query {
  private DeviceSchema deviceSchema;

  public DeviceQuery() {}

  public DeviceQuery(DeviceSchema deviceSchema) {
    this.deviceSchema = deviceSchema;
  }

  public DeviceSchema getDeviceSchema() {
    return deviceSchema;
  }

  @Override
  public StringBuilder getQueryAttrs() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("DeviceSchema=").append(deviceSchema);
    return stringBuilder;
  }
}
