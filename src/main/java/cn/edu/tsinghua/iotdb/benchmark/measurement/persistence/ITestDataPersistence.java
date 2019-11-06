package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence;

import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import java.util.Map;

public interface ITestDataPersistence {

    /**
     * Store system resources metrics data
     * @param systemMetricsMap System resources metrics to be stored
     */
    void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap);

    void saveOperationResult(String operation, int okPoint, int failPoint, double latency, String remark);

    void saveResult(String operation, String k, String v);

    void saveTestConfig();

    void close();

}
