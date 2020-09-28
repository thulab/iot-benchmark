package cn.edu.tsinghua.iotdb.benchmark.measurement.enums;

public enum SingleTestMetrics {
    OK_POINT("okPoint","INT32"),
    FAIL_POINT("failPoint","INT32"),
    LATENCY("latency","DOUBLE"),
    REMARK("remark","TEXT");

    String name;
    String type;

    SingleTestMetrics(String n, String t) {
        name = n;
        type = t;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }
}
