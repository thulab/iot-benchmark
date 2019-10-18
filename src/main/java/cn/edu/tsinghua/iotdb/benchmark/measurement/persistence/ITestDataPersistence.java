package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence;

import java.util.List;

public interface ITestDataPersistence {

    /**
     * store the system resource consumption data
     * @param cpu CPU usage
     * @param mem memory usage
     * @param io I/O usage
     * @param networkReceive network receive speed
     * @param networkSend network send speed
     * @param processMemSize memory consumption size of the DB service process
     * @param dataSize data dir size
     * @param systemSize system dir size
     * @param sequenceSize sequence dir size
     * @param overflowSize un-sequence dir size
     * @param walSize WAL dir size
     * @param tps I/O TPS
     * @param ioRead I/O read speed
     * @param ioWrite I/O write speed
     * @param openFileList Open file number of different dir
     */
    void insertSystemMetrics(double cpu, double mem, double io, double networkReceive, double networkSend, double processMemSize,
        double dataSize, double systemSize, double sequenceSize, double overflowSize, double walSize,
        float tps, float ioRead, float ioWrite, List<Integer> openFileList);

    /**
     *
     * @param operation
     * @param okPoint
     * @param failPoint
     * @param latency
     * @param remark
     */
    void saveOperationResult(String operation, int okPoint, int failPoint, double latency, String remark);

    void saveResult(String operation, String k, String v);

    void saveTestConfig();

    void close();

}
