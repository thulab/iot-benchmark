package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

import java.util.List;

public class Record {
  private long timestamp;
  private List<String> recordDataValue;

  public Record(long timestamp, List<String> recordDataValue) {
    this.timestamp = timestamp;
    this.recordDataValue = recordDataValue;
  }

  public int size(){
    return recordDataValue.size();
  }


  public long getTimestamp() {
    return timestamp;
  }

  public List<String> getRecordDataValue() {
    return recordDataValue;
  }

}
