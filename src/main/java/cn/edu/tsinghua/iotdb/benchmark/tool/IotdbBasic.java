package cn.edu.tsinghua.iotdb.benchmark.tool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.iotdb.IoTDB;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

public class IotdbBasic {
	private Config config;
	private Connection connection;
	private DatabaseMetaData databaseMetaData;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
	private static final String createTimeseriesSQL = "create timeseries %s with datatype=%s,encoding=%s";
	private static final String setStorageLevelSQL = "set storage group to %s";
	
	public IotdbBasic() throws ClassNotFoundException, SQLException {
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		config = ConfigDescriptor.getInstance().getConfig();	
	}
	
	public void init() throws SQLException {
		connection = DriverManager.getConnection(String.format(Constants.URL, config.host, config.port), Constants.USER,
				Constants.PASSWD);
		databaseMetaData = null;
	}
	
	public void setStorgeGroup(String prefixPath) {
		Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute(String.format(setStorageLevelSQL, prefixPath));
			statement.close();
		} catch (SQLException e) {
			LOGGER.error("An error occurred while creating storage group : "+e.getMessage());
			e.printStackTrace();
		}
	}

	public void createTimeseries(String prefixPath, String sensor, String type, String encoding) {
		createTimeseries(prefixPath + "." + sensor, type, encoding);
	}

	public void createTimeseries(String path, String type, String encoding) {
		Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute(String.format(createTimeseriesSQL,path, type, encoding));
			statement.close();
		} catch (SQLException e) {
			LOGGER.warn(e.getMessage());
		}
	}
	public void insertOneBatch(List<String> sqls, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) {
		Statement statement;
		int[] result;
		long errorNum = 0;
		try {
			statement = connection.createStatement();
			for(String sql : sqls){
				statement.addBatch(sql);
			}
			
			long startTime = System.currentTimeMillis();
			result = statement.executeBatch();
			statement.clearBatch();
			statement.close();
			long endTime = System.currentTimeMillis();
			long costTime = endTime - startTime;
			for (int i = 0; i < result.length; i++) {
				if (result[i] == -1) {
					errorNum++;
				}
			}
			if (errorNum > 0) {
				LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);

			} else {
				totalTime.set(totalTime.get() + costTime);
			}
			errorCount.set(errorCount.get() + errorNum);

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public String queryTimeseriesDataType(String name){
		String type = "";
		try {
			databaseMetaData = getDatabaseMetaData();
			ResultSet resultSet = databaseMetaData.getColumns(null, null, name, null);
			if (resultSet.next()) {
				type = resultSet.getString(1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return type;
	}
	
	public DatabaseMetaData getDatabaseMetaData() {
		if(databaseMetaData == null){
			try {
				databaseMetaData = connection.getMetaData();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				LOGGER.error("Error occurs when getDatabaseMetaData, {}",e.getMessage());
				e.printStackTrace();
			}
		}
		return databaseMetaData;
	}

	public void close() throws SQLException {
		if (connection != null) {
			connection.close();
		}
	}
}
