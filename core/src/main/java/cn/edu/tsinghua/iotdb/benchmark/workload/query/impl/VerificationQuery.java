package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;

import java.util.List;

public class VerificationQuery extends Query {
  private DeviceSchema deviceSchema;
  private List<Record> records;

  public VerificationQuery() {}

  public VerificationQuery(Batch batch) {
    this.deviceSchema = batch.getDeviceSchema();
    this.records = batch.getRecords();
  }

  public DeviceSchema getDeviceSchema() {
    return deviceSchema;
  }

  public List<Record> getRecords() {
    return records;
  }

  /**
   * get attributes of query
   *
   * @return
   */
  @Override
  public StringBuilder getQueryAttrs() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("deviceSchema=").append(deviceSchema);
    stringBuilder.append(" records=").append(records);
    return stringBuilder;
  }
}
