package cn.edu.tsinghua.iotdb.benchmark.workload.enums;

public enum OutOfOrderMode {
  POISSON(0),
  BATCH(1);

  private int mode;

  OutOfOrderMode(int mode) {
    this.mode = mode;
  }

  public static OutOfOrderMode getOutOfOrderMode(int mode) {
    for (OutOfOrderMode outOfOrderMode : OutOfOrderMode.values()) {
      if (mode == outOfOrderMode.mode) {
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
