package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;

public interface IDatebase {
	void createConnection() throws SQLException;
	void createSchema() throws SQLException;
	void insert() throws SQLException;
	void close() throws SQLException;
	
}
