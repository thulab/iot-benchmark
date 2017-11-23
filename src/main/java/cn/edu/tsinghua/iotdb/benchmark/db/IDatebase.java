package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;
import java.util.LinkedList;

public interface IDatebase {
	void init() throws SQLException;
	void createSchema() throws SQLException;
	void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException;
	void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException;
	void close() throws SQLException;
	//void executeOneQuery(String device, int batchIndex,ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount);
	void executeOneQuery(String device, int index, long startTime,
			QueryClientThread client, ThreadLocal<Long> errorCount);
	void flush() throws SQLException;;
	long getTotalTimeInterval() throws SQLException;
}
