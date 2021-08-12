package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

public enum Type {
  BOOLEAN(1, "BOOLEAN"),
  INT32(2, "INT32"),
  INT64(3, "INT64"),
  FLOAT(4, "FLOAT"),
  DOUBLE(5, "DOUBLE"),
  TEXT(6, "TEXT");

  public int index;
  public String name;

  Type(int index, String name) {
    this.index = index;
    this.name = name;
  }

  public static Type[] getValueTypes() {
    Type type[] = new Type[4];
    for (int i = 1; i < 5; i++) {
      type[i - 1] = Type.values()[i];
    }
    return type;
  }

  public static Type getType(int index) {
    for (Type type : Type.values()) {
      if (type.index == index) {
        return type;
      }
    }
    // default type
    return Type.TEXT;
  }

  @Override
  public String toString() {
    return name;
  }
}
