package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

import java.util.List;

public class Record {
  private long timestamp;
  private List<Object> recordDataValue;

  public Record(long timestamp, List<Object> recordDataValue) {
    this.timestamp = timestamp;
    this.recordDataValue = recordDataValue;
  }

  public int size(){
    return recordDataValue.size();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public List<Object> getRecordDataValue() {
    return recordDataValue;
  }

}
