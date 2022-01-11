package cn.edu.tsinghua.iotdb.benchmark.entity;

public class DeviceSummary {
  /** The name of device */
  private String device;
  /** Total number of line */
  private int totalLineNumber = 0;
  /** Min timestamp */
  private long minTimeStamp = 0;
  /** Max timestamp */
  private long maxTimeStamp = 0;

  public DeviceSummary(String device, int totalLineNumber, long minTimeStamp, long maxTimeStamp) {
    this.device = device;
    this.totalLineNumber = totalLineNumber;
    this.minTimeStamp = minTimeStamp;
    this.maxTimeStamp = maxTimeStamp;
  }

  public String getDevice() {
    return device;
  }

  public int getTotalLineNumber() {
    return totalLineNumber;
  }

  public long getMinTimeStamp() {
    return minTimeStamp;
  }

  public long getMaxTimeStamp() {
    return maxTimeStamp;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DeviceSummary)) {
      return false;
    }
    DeviceSummary anotherDeviceSummary = (DeviceSummary) obj;
    return device.equals(anotherDeviceSummary.getDevice())
        && totalLineNumber == anotherDeviceSummary.getTotalLineNumber()
        && minTimeStamp == anotherDeviceSummary.getMinTimeStamp()
        && maxTimeStamp == anotherDeviceSummary.getMaxTimeStamp();
  }

  @Override
  public String toString() {
    return "DeviceSummary[device="
        + device
        + ", totalLineNumber="
        + totalLineNumber
        + ", minTimeStamp="
        + minTimeStamp
        + ", maxTimeStamp="
        + maxTimeStamp
        + "]";
  }
}
