package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;
import java.util.LinkedList;

public interface IDatebase {
	void init() throws SQLException;
	void createSchema() throws SQLException;
<<<<<<< HEAD
	void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime) throws SQLException;
	void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime) throws SQLException;
=======
	long insertOneBatch(String device, int batchIndex, long totalTime) throws SQLException;
>>>>>>> cd4bf755e8391ee899a9933d23f2fe08456e34f6
	void close() throws SQLException;
	
}
