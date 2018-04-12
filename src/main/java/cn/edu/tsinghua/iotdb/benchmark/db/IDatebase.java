package cn.edu.tsinghua.iotdb.benchmark.db;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public interface IDatebase {
    void init() throws SQLException;

    void createSchema() throws SQLException;

    long getLabID();

    void insertOneBatch(String device,
                        int batchIndex,
                        ThreadLocal<Long> totalTime,
                        ThreadLocal<Long> errorCount) throws SQLException;

    void insertOneBatch(LinkedList<String> cons,
                        int batchIndex,
                        ThreadLocal<Long> totalTime,
                        ThreadLocal<Long> errorCount) throws SQLException;

    void close() throws SQLException;

    void flush() throws SQLException;

    void getUnitPointStorageSize() throws SQLException;

    long getTotalTimeInterval() throws SQLException;

    void executeOneQuery(List<Integer> devices,
                         int index,
                         long startTime,
                         QueryClientThread client,
                         ThreadLocal<Long> errorCount);

    void insertOneBatchMulDevice(LinkedList<String> deviceCodes,
                                 int batchIndex,
                                 ThreadLocal<Long> totalTime,
                                 ThreadLocal<Long> errorCount) throws SQLException;

    long count(String group, String device, String sensor);

    void createSchemaOfDataGen() throws SQLException;

    void insertGenDataOneBatch(String device,
                               int i,
                               ThreadLocal<Long> totalTime,
                               ThreadLocal<Long> errorCount) throws SQLException;

    void exeSQLFromFileByOneBatch() throws SQLException, IOException;

    int insertOverflowOneBatch(String device,
                               int loopIndex,
                               ThreadLocal<Long> totalTime,
                               ThreadLocal<Long> errorCount,
                               ArrayList<Integer> before,
                               Integer maxTimestampIndex,
                               Random random) throws SQLException;

    int insertOverflowOneBatchDist(String device,
                               int loopIndex,
                               ThreadLocal<Long> totalTime,
                               ThreadLocal<Long> errorCount,
                               Integer maxTimestampIndex,
                               Random random) throws SQLException;
}
