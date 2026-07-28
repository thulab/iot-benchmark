package cn.edu.tsinghua.iot.benchmark.tsdb;

/** Timings of the two distinct phases of a TsFile LOAD benchmark. */
public class TsFileLoadResult {
  private final long buildNanos;
  private final long loadNanos;
  private final long transferNanos;
  private final long points;

  public TsFileLoadResult(long buildNanos, long transferNanos, long loadNanos, long points) {
    this.buildNanos = buildNanos;
    this.transferNanos = transferNanos;
    this.loadNanos = loadNanos;
    this.points = points;
  }

  public long getBuildNanos() {
    return buildNanos;
  }

  public long getLoadNanos() {
    return loadNanos;
  }

  public long getTransferNanos() {
    return transferNanos;
  }

  public long getPoints() {
    return points;
  }
}
