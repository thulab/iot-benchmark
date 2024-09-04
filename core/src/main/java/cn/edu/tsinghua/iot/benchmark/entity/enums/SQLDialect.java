package cn.edu.tsinghua.iot.benchmark.entity.enums;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum SQLDialect {
  TREE("tree"),
  TABLE("table");

  final String name;

  SQLDialect(String name) {
    this.name = name;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SQLDialect.class);

  public static SQLDialect getSQLDialectByName(String name) {
    switch (name) {
      case "tree":
        return SQLDialect.TREE;
      case "table":
        return SQLDialect.TABLE;
      default:
        throw new UnsupportedOperationException(name + ": This sql dialect is not supported.");
    }
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
