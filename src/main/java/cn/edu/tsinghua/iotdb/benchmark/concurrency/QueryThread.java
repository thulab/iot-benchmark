package cn.edu.tsinghua.iotdb.benchmark.concurrency;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryThread implements Runnable {
	private Connection connection;
	private Statement statement;
	private static final Logger LOGGER = LoggerFactory.getLogger(QueryThread.class);
	public String url;
	
	public QueryThread(String url){
		this.url = url;
	}
	
	@Override
	public void run() {
		try {
			connection =  DriverManager.getConnection(url,
					ConcurrentConfig.USER_NAME,
					ConcurrentConfig.PASSWORD);
			Statement statement = connection.createStatement();
			int i = 0;
			while(true){
				statement.execute(ConcurrentConfig.QUERY_SQL);
				ResultSet resultSet = statement.getResultSet();
				while(resultSet.next()){
				}
				statement.close();
				i++;
				LOGGER.info("{} executes {} times",
						Thread.currentThread().getId(),
						i);
			}
		} catch (Exception e) {
			LOGGER.error("{} encouters an exception at {} because of {}", 
					Thread.currentThread().getId(), 
					url,
					e.getMessage());
		} finally {
			if(statement != null){
				try {
					statement.close();
				} catch (SQLException e) {
					LOGGER.error("{} encouters an exception when closing statment because of {}", 
							Thread.currentThread().getId(), 
							e.getMessage());
				}
			}
			if(connection != null){
				try {
					connection.close();
				} catch (SQLException e) {
					LOGGER.error("{} encouters an exception when closing connection because of {}", 
							Thread.currentThread().getId(), 
							e.getMessage());
				}
			}
		}
		LOGGER.info("{} exit", Thread.currentThread().getId());
	}

}
