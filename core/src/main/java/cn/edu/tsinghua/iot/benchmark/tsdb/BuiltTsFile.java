package cn.edu.tsinghua.iot.benchmark.tsdb;

import java.io.File;

/** A locally constructed TsFile waiting to be staged and LOADed. */
public final class BuiltTsFile {
  private final File file;
  private final long buildNanos;
  private final long points;
  // Table-model LOAD is scoped by the active database, unlike tree-model LOAD.
  private final String database;

  public BuiltTsFile(File file, long buildNanos, long points) {
    this(file, buildNanos, points, null);
  }

  public BuiltTsFile(File file, long buildNanos, long points, String database) {
    this.file = file;
    this.buildNanos = buildNanos;
    this.points = points;
    this.database = database;
  }

  public File getFile() {
    return file;
  }

  public long getBuildNanos() {
    return buildNanos;
  }

  public long getPoints() {
    return points;
  }

  public String getDatabase() {
    return database;
  }
}
