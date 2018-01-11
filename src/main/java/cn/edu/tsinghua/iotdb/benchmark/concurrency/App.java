package cn.edu.tsinghua.iotdb.benchmark.concurrency;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.CommandCli;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

public class App {

	public static List<String> allTimeSeries = null;
	private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
	public static void main(String[] args) throws ClassNotFoundException {
		CommandCli cli = new CommandCli();
		if (!cli.init(args)) {
			return;
		}
		Config config = ConfigDescriptor.getInstance().getConfig();
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		getAllTimeSeries();
		ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
		for (int i = 0; i < config.MAX_CONNECTION_NUM; i++) {
			cachedThreadPool.execute(new QueryThread(config.CONCURRENCY_URL));
			LOGGER.info("Create thread {}, the url is {}!",i,config.CONCURRENCY_URL);
		}

	}

	public static List<String> getAllTimeSeries() {
		if(allTimeSeries != null){
			return allTimeSeries;
		}
		Config config = ConfigDescriptor.getInstance().getConfig();
		allTimeSeries = new ArrayList<String>();
		if(config.CONCURRENCY_QUERY_FULL_DATA){
			allTimeSeries.add("root");
			return allTimeSeries;
		}
		LOGGER.info("start query timeSeries info ...");
		Connection connection = null;
		
		try {
			connection = DriverManager.getConnection(config.CONCURRENCY_URL, "root", "root");
			ResultSet resultSet = connection.getMetaData().getColumns(null,
					null, "root.*", null);
			Set<String> tmp = new HashSet<String>();
			while (resultSet.next()) {
				tmp.add(resultSet.getString(1).substring(0, resultSet.getString(1).lastIndexOf('.')));
				//System.out.println(resultSet.getString(1));
			}
			for (String str : tmp){
				allTimeSeries.add(str);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LOGGER.error("Meet error in get timeSeries path, because {}",e.getMessage());
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					LOGGER.error("{} encouters an exception when closing connection because of {}",
							Thread.currentThread().getId(), e.getMessage());
									
				}
			}
		}//finally
		LOGGER.info("Get all timeseries path ...");
		return allTimeSeries;
	}

}
