package cn.edu.tsinghua.iotdb.benchmark.measurement.enums;

public enum TotalResult {
    CREATE_SCHEMA_TIME("createSchemaTime"),
    ELAPSED_TIME("elapsedTime");

    String name;

    TotalResult(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
