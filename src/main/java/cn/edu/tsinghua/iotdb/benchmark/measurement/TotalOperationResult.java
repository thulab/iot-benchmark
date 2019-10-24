package cn.edu.tsinghua.iotdb.benchmark.measurement;

public enum TotalOperationResult {
    OK_OPERATION_NUM("okOperationNum"),
    OK_POINT_NUM("okPointNum"),
    FAIL_POINT_NUM("failPointNum"),
    FAIL_OPERATION_NUM("failOperationNum"),
    THROUGHPUT("throughput");

    String name;

    TotalOperationResult(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
