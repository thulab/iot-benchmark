package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

public class TimeValuePair {

  private long timestamp;
  private double value;

  TimeValuePair(long timestamp, double value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }
}
