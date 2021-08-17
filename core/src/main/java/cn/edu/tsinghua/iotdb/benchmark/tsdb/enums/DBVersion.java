package cn.edu.tsinghua.iotdb.benchmark.tsdb.enums;

public enum DBVersion {
  IOTDB_012("012"),
  IOTDB_011("011"),
  IOTDB_010("010"),
  IOTDB_09("09"),
  InfluxDB_2("2.0");

  String version;

  DBVersion(String version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return version;
  }
}
