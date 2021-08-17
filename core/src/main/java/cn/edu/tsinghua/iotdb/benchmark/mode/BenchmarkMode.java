package cn.edu.tsinghua.iotdb.benchmark.mode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum BenchmarkMode {
  TEST_WITH_DEFAULT_PATH("testWithDefaultPath"),
  GENERATE_DATA("generateDataMode"),
  VERIFICATION("verificationMode"),
  SERVER("serverMODE");

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkMode.class);
  public String mode;

  BenchmarkMode(String mode) {
    mode = mode.trim();
    this.mode = mode;
  }

  public static BenchmarkMode getBenchmarkMode(String mode) {
    for (BenchmarkMode benchmarkMode : BenchmarkMode.values()) {
      if (benchmarkMode.mode.equals(mode)) {
        return benchmarkMode;
      }
    }
    BenchmarkMode defaultBenchmark = BenchmarkMode.TEST_WITH_DEFAULT_PATH;
    LOGGER.warn("Using Benchmark Mode: " + defaultBenchmark.mode);
    return defaultBenchmark;
  }

  @Override
  public String toString() {
    return mode;
  }
}
