package cn.edu.tsinghua.iotdb.benchmark.db;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public interface IDatebase {
	void init() throws SQLException;
	void createSchema() throws SQLException;
	void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException;
	void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException;
	void close() throws SQLException;

	void flush() throws SQLException;
	void getUnitPointStorageSize() throws SQLException;
	long getTotalTimeInterval() throws SQLException;
	void executeOneQuery(List<Integer> devices, int index, long startTime,
			QueryClientThread client, ThreadLocal<Long> errorCount);
	void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException;
	long count(String group, String device, String sensor);

	void createSchemaOfDataGen() throws SQLException;

	void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException;

    int exeSQLFromFileByOneBatch() throws SQLException, IOException;
}
