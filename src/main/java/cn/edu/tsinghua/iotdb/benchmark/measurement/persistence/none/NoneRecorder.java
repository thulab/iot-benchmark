package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.none;

import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import java.util.List;

public class NoneRecorder implements ITestDataPersistence {

    @Override
    public void insertSystemMetrics(double cpu, double mem, double io, double networkReceive, double networkSend,
        double processMemSize, double dataSize, double systemSize, double sequenceSize, double overflowSize,
        double walSize, float tps, float ioRead, float ioWrite, List<Integer> openFileList) {

    }

    @Override
    public void saveOperationResult(String operation, int okPoint, int failPoint, double latency, String remark) {

    }

    @Override public void saveResult(String operation, String k, String v) {

    }

    @Override public void saveTestConfig() {

    }

    @Override public void close() {

    }
}
