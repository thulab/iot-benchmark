package cn.edu.tsinghua.iotdb.benchmark.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Point;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

public class IoTDB implements IDatebase {
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);

	private static final String createStatementSQL = "create timeseries %s with datatype=DOUBLE,encoding=GORILLA";
	private static final String createStatementFromFileSQL = "create timeseries %s with datatype=%s,encoding=%s";
	private static final String setStorageLevelSQL = "set storage group to %s";
	private Connection connection;
	private Config config;
	private List<Point> points;
	private Map<String, String> mp;

	public IoTDB() throws ClassNotFoundException, SQLException {
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		config = ConfigDescriptor.getInstance().getConfig();
		points = new ArrayList<>();
		mp = new HashMap<>();
	}

	@Override
	public void createSchema() throws SQLException {
		if (config.READ_FROM_FILE) {
			initSchema();
			mp.put("INT32", "PLAIN");
			mp.put("INT64", "PLAIN");
			mp.put("DOUBLE", "GORILLA");
			if (config.TAG_PATH) {
				if (config.STORE_MODE == 1) {
					for (Point p : points) {
						setStorgeGroup(p.getPath());
					}
				} else {
					Set<String> uniqueMeasurementName = new HashSet<>();
					for (Point p : points) {
						uniqueMeasurementName.add(p.measurement);
					}
					for (String measure : uniqueMeasurementName) {
						setStorgeGroup("root." + measure);
					}
				}

				for (Point p : points) {
					Set<String> uniqueFieldName = new HashSet<>();
					uniqueFieldName.addAll(p.fieldName);
					for (String sensor : uniqueFieldName) {
						createTimeseries(p.getPath(), sensor);
					}
				}
			} else {
				setStorgeGroup("root.device_0");
				Set<String> uniqueFieldName = new HashSet<>();
				for (Point p : points) {
					uniqueFieldName.addAll(p.fieldName);
				}
				for (String sensor : uniqueFieldName) {
					createTimeseries("root.device_0", sensor);
				}
			}
		} else {
			int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
			ArrayList<String> group = new ArrayList<>();
			for (int i = 0; i < config.GROUP_NUMBER; i++) {
				group.add("group_" + i);
			}
			for (String g : group) {
				setStorgeGroup(g);
			}
			int count = 0;
			int groupIndex = 0;
			int timeseriesCount = 0;
			Statement statement = connection.createStatement();
			int timeseriesTotal = config.DEVICE_NUMBER * config.SENSOR_NUMBER;
			String path;
			for (String device : config.DEVICE_CODES) {
				if (count == groupSize) {
					groupIndex++;
					count = 0;
				}
				path = group.get(groupIndex) + "." + device;
				for (String sensor : config.SENSOR_CODES) {
					//createTimeseries(path, sensor);
					timeseriesCount++;
					createTimeseriesBatch(path, sensor, timeseriesCount, timeseriesTotal,statement);
				}
				count++;
			}
		}

	}

	private void initSchema() {
		// 解析到points
		BufferedReader reader = null;
		try {
			File file = new File(config.FILE_PATH);
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;

			// 读到指定的行数或者文件结束
			while ((tempString = reader.readLine()) != null) {

				Point p = new Point();

				String[] pointInfo = tempString.split(" ");
				StringTokenizer st = new StringTokenizer(pointInfo[0], ",=");

				// 解析出measurement
				if (st.hasMoreElements())
					p.measurement = st.nextToken().replace('.', '_');

				// 解析出tag的K-V对
				while (st.hasMoreElements()) {
					p.tagName.add(st.nextToken().replace('.', '_'));
					p.tagValue.add(st.nextToken().replace('.', '_')
							.replace('/', '_'));
				}

				// 解析出field的K-V对
				st = new StringTokenizer(pointInfo[1], ",=");
				while (st.hasMoreElements()) {
					p.fieldName.add(st.nextToken());
					p.fieldValue.add(string2num(st.nextToken()));
				}

				p.time = Long.parseLong(pointInfo[2]);
				if (points.size() == 0
						|| !points.get(0).getPath().equals(p.getPath())) {
					points.add(p);
				} else {
					break;
				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}

	private static Number string2num(String str) {
		// TODO Auto-generated method stub
		if (str.endsWith("i")) {
			return Long.parseLong(str.substring(0, str.length() - 1));
		} else {
			return Double.parseDouble(str);
		}
	}

	@Override
	public void insertOneBatch(String device, int loopIndex,
			ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) {
		Statement statement;
		try {
			statement = connection.createStatement();
			for (int i = 0; i < config.CACHE_NUM; i++) {
				String sql = createSQLStatment(loopIndex, i, device);
				statement.addBatch(sql);
			}
			long startTime = System.currentTimeMillis();
			statement.executeBatch();
			statement.clearBatch();
			statement.close();
			long endTime = System.currentTimeMillis();
			LOGGER.info(
					"{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
					Thread.currentThread().getName(),
					loopIndex,
					(endTime - startTime) / 1000.0,
					(totalTime.get() + (endTime - startTime)) / 1000.0,
					(config.CACHE_NUM * config.SENSOR_NUMBER / (double) (endTime - startTime)) * 1000);
			totalTime.set(totalTime.get() + (endTime - startTime));
		} catch (BatchUpdateException e) {
			// TODO Auto-generated catch block
			int[] ret = e.getUpdateCounts();
			int diff = config.CACHE_NUM - ret.length;
			if (diff == 0) {// same--遇到error继续执行；不同则说明只返回了正确的个数；
				for (int i : ret) {
					if (i == Statement.EXECUTE_FAILED) {
						diff++;
					}
				}
			}
			errorCount.set(errorCount.get() + diff);
			LOGGER.error("Batch insert failed, the failed num is {}! Error：{}",
					diff, e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void insertOneBatch(LinkedList<String> cons, int batchIndex,
			ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount)
			throws SQLException {
		// TODO Auto-generated method stub

		Statement statement;
		try {
			statement = connection.createStatement();
			for (String sql : cons) {
				statement.addBatch(sql);
			}

			long startTime = System.currentTimeMillis();
			statement.executeBatch();
			statement.clearBatch();
			statement.close();
			long endTime = System.currentTimeMillis();

			LOGGER.debug(
					"{} execute {} loop, it costs {}s, totalTime {}s, throughput {} items/s",
					Thread.currentThread().getName(), batchIndex,
					(endTime - startTime) / 1000.0,
					((totalTime.get() + (endTime - startTime)) / 1000.0),
					(cons.size() / (double) (endTime - startTime)) * 1000);
			totalTime.set(totalTime.get() + (endTime - startTime));
		} catch (BatchUpdateException e) {
			// TODO Auto-generated catch block
			int[] ret = e.getUpdateCounts();
			int diff = cons.size() - ret.length;
			if (diff == 0) {// same--遇到error继续执行；不同则说明只返回了正确的个数；
				for (int i : ret) {
					if (i == Statement.EXECUTE_FAILED) {
						diff++;
					}
				}
			}
			errorCount.set(errorCount.get() + diff);
			LOGGER.error("Batch insert failed, the failed num is {}! Error：{}",
					diff, e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void executeOneQuery(String device, int index,
			ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) {
		Statement statement;
		try {
			statement = connection.createStatement();
			String sql = "";
			switch(config.QUERY_CHOICE){
				case 1:
					sql = createQuerySQLStatment(device);
					break;
				case 2:
					sql = createQuerySQLStatment(device, config.QUERY_SENSOR_NUM);
					break;
				case 3:
					sql = createQuerySQLStatment(device, config.QUERY_AGGREGATE_FUN);
					break;
				case 4:
					long startTime = Constants.START_TIMESTAMP + config.POINT_STEP
							* (index * config.CACHE_NUM + index);
					sql = createQuerySQLStatment(device, startTime, startTime + config.POINT_STEP * config.CACHE_NUM );
					break;	
			}
			long startTime = System.currentTimeMillis();
			statement.execute(sql);
	        ResultSet resultSet = statement.getResultSet();
			statement.close();
			long endTime = System.currentTimeMillis();
			LOGGER.info(
					"{} execute {} loop, it costs {}s, totalTime {}s",
					Thread.currentThread().getName(),
					index,
					(endTime - startTime) / 1000.0,
					(totalTime.get() + (endTime - startTime)) / 1000.0);
			totalTime.set(totalTime.get() + (endTime - startTime));
		} catch (SQLException e) {
			errorCount.set(errorCount.get() + 1);
			LOGGER.error("{} execute query failed! Error：{}",Thread.currentThread().getName(),e.getMessage());
			e.printStackTrace();
		}
	}

	private void createTimeseries(String path, String sensor) {
		Statement statement;
		try {
			statement = connection.createStatement();
			if (config.READ_FROM_FILE) {
				String type = getTypeByField(sensor);
				statement.execute(String.format(createStatementFromFileSQL,
						path + "." + sensor, type, mp.get(type)));
			} else {
				statement
						.execute(String.format(createStatementSQL,
								Constants.ROOT_SERIES_NAME + "." + path + "."
										+ sensor));
			}
			statement.close();
		} catch (SQLException e) {
			LOGGER.warn(e.getMessage());
		}

	}

	private  void createTimeseriesBatch(String path, String sensor, int count, int timeseriesTotal, Statement statement){
		try {
			statement.addBatch(String.format(createStatementSQL,
					Constants.ROOT_SERIES_NAME + "." + path + "."
							+ sensor));
			if((count%1000)==0){
				statement.executeBatch();
				statement.clearBatch();
				if(count>=timeseriesTotal){
					statement.close();
				}
				//statement.close();
			}else if(count>=timeseriesTotal){
				statement.executeBatch();
				statement.clearBatch();
				statement.close();
			}
		} catch (SQLException e) {
			LOGGER.warn(e.getMessage());
		}
	}

	private void setStorgeGroup(String device) {
		Statement statement;
		try {
			statement = connection.createStatement();
			if (config.READ_FROM_FILE) {
				statement.execute(String.format(setStorageLevelSQL, device));
			} else {
				statement.execute(String.format(setStorageLevelSQL,
						Constants.ROOT_SERIES_NAME + "." + device));
			}
			statement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void init() throws SQLException {
		connection = DriverManager.getConnection(
				String.format(Constants.URL, config.host, config.port),
				Constants.USER, Constants.PASSWD);
	}

	@Override
	public void close() throws SQLException {
		if (connection != null) {
			connection.close();
		}
	}

	private String createSQLStatment(int batch, int index, String device) {
		StringBuilder builder = new StringBuilder();
		String path = getGroupDevicePath(device);
		builder.append("insert into ").append(Constants.ROOT_SERIES_NAME)
				.append(".").append(path).append("(timestamp");

		for (String sensor : config.SENSOR_CODES) {
			builder.append(",").append(sensor);
		}
		builder.append(") values(");
		long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP
				* (batch * config.CACHE_NUM + index);
		builder.append(currentTime);
		for (String sensor : config.SENSOR_CODES) {
			FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
			builder.append(",").append(
					Function.getValueByFuntionidAndParam(param, currentTime));
		}
		builder.append(")");
		return builder.toString();
	}
	
	/**创建查询语句--(查询设备下的所有传感器数值)*/
	private String createQuerySQLStatment(String device){
		StringBuilder builder = new StringBuilder();
		String path = getGroupDevicePath(device);
		
		builder.append("select * from ").append(Constants.ROOT_SERIES_NAME)
				.append(".").append(path);
		return builder.toString();
	}
	
	/**创建查询语句--(查询设备下的num个传感器数值)
	 * @throws SQLException */
	private String createQuerySQLStatment(String device, int num) throws SQLException{
		StringBuilder builder = new StringBuilder();
		String path = getGroupDevicePath(device);
		builder.append("select ");
		if(num > config.SENSOR_NUMBER){
			throw new SQLException("config.SENSOR_NUMBER is " + config.SENSOR_NUMBER + 
					" shouldn't less than the number of fields in querySql");
		}
		List<String> list=new ArrayList<String>();  
		for (String sensor : config.SENSOR_CODES) {
			list.add(sensor);
		}
		Collections.shuffle(list); 
		builder.append(list.get(0));
		for(int i = 1; i < num; i++){
			builder.append(" , ").append(list.get(i));
		}
		builder.append(" from ").append(Constants.ROOT_SERIES_NAME)
				.append(".").append(path);
		return builder.toString();
	}
	
	/**创建查询语句--(带有聚合函数的查询)*/
	private String createQuerySQLStatment(String device, String method){
		StringBuilder builder = new StringBuilder();
		String path = getGroupDevicePath(device);
		int sensorIndex =  (int) (System.currentTimeMillis() % config.SENSOR_NUMBER);
		builder.append("select ").append(method).append("(").append(config.SENSOR_CODES.get(sensorIndex)).append(")");
		builder.append(" from ").append(Constants.ROOT_SERIES_NAME)
				.append(".").append(path);
		return builder.toString();
	}
	
	/**创建查询语句--(带有时间约束条件的查询)
	 * @throws SQLException */
	private String createQuerySQLStatment(String device, long startTime, long endTime) throws SQLException{
		StringBuilder builder = new StringBuilder();
		builder.append(createQuerySQLStatment(device, 1)).append(" where time > ");
		builder.append(startTime).append(" AND time < ").append(endTime);
		return builder.toString();
	}
	

	String getGroupDevicePath(String device){
		String[] spl = device.split("_");
		int deviceIndex = Integer.parseInt(spl[1]);
		int groupSize = config.DEVICE_NUMBER/config.GROUP_NUMBER;
		int groupIndex = deviceIndex/groupSize;
		return "group_"+groupIndex+"."+device;
	}

	private String getTypeByField(String name) {
		if (name.endsWith("_percent") || name.startsWith("usage_")) {
			return "DOUBLE";
		}
		return "INT64";
	}

}
