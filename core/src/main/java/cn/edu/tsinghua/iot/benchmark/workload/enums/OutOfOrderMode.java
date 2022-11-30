package cn.edu.tsinghua.iot.benchmark.workload.enums;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum OutOfOrderMode {
  POISSON,
  BATCH;

  private static final Logger LOGGER = LoggerFactory.getLogger(OutOfOrderMode.class);

  public static OutOfOrderMode getOutOfOrderMode(String name) {
    for (OutOfOrderMode outOfOrderMode : OutOfOrderMode.values()) {
      if (name.equals(outOfOrderMode.name())) {
        return outOfOrderMode;
      }
    }
    LOGGER.warn("Unknown out of order mode: " + name + ", use possion.");
    return OutOfOrderMode.POISSON;
  }

  @Override
  public String toString() {
    return name();
  }
}
