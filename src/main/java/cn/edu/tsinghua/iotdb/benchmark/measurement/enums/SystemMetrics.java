package cn.edu.tsinghua.iotdb.benchmark.measurement.enums;

public enum SystemMetrics {
    /*
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
     */

    CPU_USAGE,
    MEM_USAGE,
    DISK_IO_USAGE,
    NETWORK_R_RATE,
    NETWORK_S_RATE,
    PROCESS_MEM_SIZE,
    DISK_TPS,
    DISK_READ_SPEED_MB,
    DISK_WRITE_SPEED_MB,

    DATA_FILE_SIZE,
    SYSTEM_FILE_SIZE,
    SEQUENCE_FILE_SIZE,
    UN_SEQUENCE_FILE_SIZE,
    WAL_FILE_SIZE,

}
