package cn.edu.tsinghua.iotdb.benchmark.concurrency;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

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
		List<String> paths = App.getAllTimeSeries();
		if(paths== null || paths.size() == 0){
			LOGGER.info("{} exits, because the query timeSeries path set is empty!",Thread.currentThread().getId());
			return ;
		}
		int size = paths.size();
		Random random = new Random();
		String sql = "";
		try {
			connection =  DriverManager.getConnection(url,
					ConcurrentConfig.USER_NAME,
					ConcurrentConfig.PASSWORD);
			Statement statement = null;
			int i = 0;
			
			while(true){
				int count = 0;
				statement = connection.createStatement();
				sql = String.format(ConcurrentConfig.QUERY_SQL, paths.get(random.nextInt(size)) );
				statement.execute(sql);
				ResultSet resultSet = statement.getResultSet();
				while(resultSet.next()){
					count++;
				}
				i++;
				connection.close();
				LOGGER.info("{} executes {} times count numer {}",
						Thread.currentThread().getId(), i, count);
			}
		} catch (Exception e) {
			LOGGER.error("{} encouters an exception at {} because of {}", 
					Thread.currentThread().getId(), 
					url,
					e.getMessage());
			LOGGER.error("query statement is {} ", sql);
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
