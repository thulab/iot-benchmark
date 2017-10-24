package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;

public interface IDatebase {
	void init() throws SQLException;
	void createSchema() throws SQLException;
	void insertOneBatch(String device, int batchIndex，long totalTime) throws SQLException;
	void close() throws SQLException;
	
}
