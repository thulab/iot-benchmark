package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

public class DataPoint {

  private String sensor;
  private String device;
  private String group;
  private long timestamp;
  private String value;

  public String getSensor() {
    return sensor;
  }

  public void setSensor(String sensor) {
    this.sensor = sensor;
  }

  public String getDevice() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public DataPoint(String group, String device, String sensor, long timestamp, String value) {
    this.group = group;
    this.device = device;
    this.sensor = sensor;
    this.timestamp = timestamp;
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "[sensor:" + sensor + ", device:" + device + ", group:" + group + ", timestamp:"
        + timestamp + ", value:"
        + value + "]";
  }
}
