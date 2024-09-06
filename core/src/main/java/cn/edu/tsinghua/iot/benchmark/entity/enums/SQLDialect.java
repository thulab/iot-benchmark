package cn.edu.tsinghua.iot.benchmark.entity.enums;

public enum SQLDialect {
  TREE("tree"),
  TABLE("table");

  final String name;

  SQLDialect(String name) {
    this.name = name;
  }

  public static SQLDialect getSQLDialect(String Dialect) {
    for (SQLDialect sqlDialect : SQLDialect.values()) {
      if (sqlDialect.toString().equalsIgnoreCase(Dialect)) {
        return sqlDialect;
      }
    }
    throw new RuntimeException(
        java.lang.String.format("Parameter SQLDialect %s is not supported", Dialect));
  }
}
