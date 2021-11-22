package cn.edu.tsinghua.iotdb.benchmark.workload.enums;

public enum OutOfOrderMode {
  POISSON,
  BATCH;

  public static OutOfOrderMode getOutOfOrderMode(String name) {
    for (OutOfOrderMode outOfOrderMode : OutOfOrderMode.values()) {
      if (name.equals(outOfOrderMode.name())) {
        return outOfOrderMode;
      }
    }
    return OutOfOrderMode.POISSON;
  }

  @Override
  public String toString() {
    return name();
  }
}
