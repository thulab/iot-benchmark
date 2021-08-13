package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;

public class VerificationQuery {
  private DeviceSchema deviceSchema;
  private List<Record> records;

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
}
