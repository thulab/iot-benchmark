package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.model.InfluxDataModel;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;

import org.apache.log4j.helpers.ISO8601DateFormat;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;

/**
 * This version use influx-java api instead of simple http.
 */
public class InfluxDBV2 implements IDatebase {
	private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBV2.class);

	private String InfluxURL;
	private String InfluxDBName;
	private Config config;
	private final String DEFAULT_RP = "autogen";
	private static final String countSQL = "select count(%s) from %s where device='%s'";
	private org.influxdb.InfluxDB influxDB;
	private MySqlLog mySql;
	private long labID;
	private Random sensorRandom = null;

	public InfluxDBV2(long labID) {
		mySql = new MySqlLog();
		this.labID = labID;
	}

	@Override
	public void init() throws SQLException {
		config = ConfigDescriptor.getInstance().getConfig();
		InfluxURL = config.INFLUX_URL;
		InfluxDBName = config.INFLUX_DB_NAME;
		influxDB = org.influxdb.InfluxDBFactory.connect(InfluxURL);
		if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_QUERY_TEST_WITH_DEFAULT_PATH)) {
			if (!influxDB.databaseExists(InfluxDBName)) {
				throw new SQLException("要查询的数据库" + InfluxDBName + "不存在！");
			}
		} else {
			if (influxDB.databaseExists(InfluxDBName)) {
				influxDB.deleteDatabase(InfluxDBName);
			}
			createDatabase(InfluxDBName);
		}
		mySql.initMysql(labID);
		sensorRandom = new Random(1 + config.QUERY_SEED);
	}

	@Override
	public void createSchema() throws SQLException {
		// no need for InfluxDB
	}

	/**
	 * Create a batch of data specified by "device" and "batchIndex", and send them
	 * to InfluxDB's Java API.
	 * 
	 * @param device
	 *            a string like "d_100", which will be used in a tag "device=d_100",
	 *            and its measurement will be "group_x", while x = 100 / groupSize
	 * @param batchIndex
	 *            a test mission is divided to multiple batches, this indicates
	 *            which batch is being processed
	 * @param totalTime
	 *            to record the total time of a test mission
	 * @throws SQLException
	 */
	@Override
	public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount)
			throws SQLException {
		LinkedList<String> dataStrs = new LinkedList<>();
		BatchPoints batchPoints = BatchPoints.database(InfluxDBName).tag("async", "true").retentionPolicy(DEFAULT_RP)
				.consistency(org.influxdb.InfluxDB.ConsistencyLevel.ALL).build();
		for (int i = 0; i < config.CACHE_NUM; i++) {
			InfluxDataModel model = createDataModel(batchIndex, i, device);
			batchPoints.point(model.toInfluxPoint());
		}
		long startTime = 0, endTime = 0;
		try {
			startTime = System.currentTimeMillis();
			influxDB.write(batchPoints);
			endTime = System.currentTimeMillis();
			LOGGER.info("{} execute {} batch, it costs {}s, totalTime{}, throughput {} points/s",
					Thread.currentThread().getName(), batchIndex, (endTime - startTime) / 1000.0,
					((totalTime.get() + (endTime - startTime)) / 1000.0),
					(batchPoints.getPoints().size() / (double) (endTime - startTime)) * 1000);
			totalTime.set(totalTime.get() + (endTime - startTime));
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000.0, totalTime.get() / 1000.0, 0,
					config.REMARK);
		} catch (Exception e) {
			errorCount.set(errorCount.get() + batchPoints.getPoints().size());
			LOGGER.error("Batch insert failed, the failed num is {}! Error：{}", batchPoints.getPoints().size(),
					e.getMessage());
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000.0, totalTime.get() / 1000.0,
					batchPoints.getPoints().size(), config.REMARK);
			throw new SQLException(e.getMessage());
		}
	}

	/**
	 * Send given data "cons" to InfluxDB's REST API. E.g. send
	 * "cpu,host=server02,region=uswest load=78 1434055562000000000" as post body to
	 * "http://localhost:8086/write?db=mydb".
	 * 
	 * @param cons
	 *            each of its element represent a data record
	 * @param batchIndex
	 *            a test mission is divided to multiple batches, this indicates
	 *            which batch is being processed
	 * @param totalTime
	 *            to record the total time of a test mission
	 * @throws SQLException
	 */
	@Override
	public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime,
			ThreadLocal<Long> errorCount) throws SQLException {
		long startTime = 0, endTime = 0;
		try {
			startTime = System.currentTimeMillis();
			influxDB.write(cons);
			endTime = System.currentTimeMillis();
			LOGGER.info("{} execute {} batch, it costs {}s, totalTime{}, throughput {} items/s",
					Thread.currentThread().getName(), batchIndex, (endTime - startTime) / 1000.0,
					((totalTime.get() + (endTime - startTime)) / 1000.0),
					(cons.size() / (double) (endTime - startTime)) * 1000);
			totalTime.set(totalTime.get() + (endTime - startTime));
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000.0, totalTime.get() / 1000.0, 0,
					config.REMARK);
		} catch (Exception e) {
			errorCount.set(errorCount.get() + cons.size());
			LOGGER.error("Batch insert failed, the failed num is {}! Error：{}", cons.size(), e.getMessage());
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000.0, totalTime.get() / 1000.0, cons.size(),
					config.REMARK + e.getMessage());
			throw new SQLException(e.getMessage());
		}
	}

	@Override
	public void close() throws SQLException {
		influxDB.close();
		if (mySql != null) {
			mySql.closeMysql();
		}
	}

	/**
	 * @param databaseName
	 * @throws SQLException
	 */
	public void createDatabase(String databaseName) throws SQLException {
		influxDB.createDatabase(InfluxDBName);
	}

	/**
	 * Create a data record, e.g. cpu,host=server02,region=uswest load=78
	 * 1434055562000000000. The measurement is the device. No tags are used
	 * currently. The timestamp is calculated from batchIndex and dataIndex. Every
	 * sensor will be regarded as a field.
	 * 
	 * @param batchIndex
	 *            a test mission is divided to multiple batches, this indicates
	 *            which batch is being processed
	 * @param dataIndex
	 *            the record's position within this batch
	 * @param device
	 *            a string like "d_100", which will be used in a tag "device=d_100",
	 *            and its measurement will be "group_x", while x = 100 / groupSize
	 * @return a structure represents the data record
	 */
	private InfluxDataModel createDataModel(int batchIndex, int dataIndex, String device) {
		InfluxDataModel model = new InfluxDataModel();
		int deviceNum = getDeviceNum(device);
		int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
		int groupNum = deviceNum / groupSize;
		model.measurement = "group_" + groupNum;
		model.tagSet.put("device", device);
		long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batchIndex * config.CACHE_NUM + dataIndex);
		model.timestamp = currentTime;
		for (String sensor : config.SENSOR_CODES) {
			FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
			Number value = Function.getValueByFuntionidAndParam(param, currentTime);
			model.fields.put(sensor, value);
		}
		return model;
	}

	private int getDeviceNum(String device) {
		String[] parts = device.split("_");
		try {
			return Integer.parseInt(parts[1]);
		} catch (Exception e) {
			LOGGER.error("{} {}", device, e.getMessage());
			throw e;
		}
	}

	static public void main(String[] args) throws SQLException {
		InfluxDB influxDB = new InfluxDB();
		influxDB.init();
		ThreadLocal<Long> time = new ThreadLocal<>();
		time.set((long) 0);
		ThreadLocal<Long> errorCount = new ThreadLocal<>();
		errorCount.set((long) 0);
		influxDB.insertOneBatch("D_0", 0, time, errorCount);
	}

	@Override
	/** 返回第一个设备的第一个传感器记录的时间跨度 */
	public long getTotalTimeInterval() throws SQLException {
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = null;
		long startTime = Constants.START_TIMESTAMP;
		long endTime = Constants.START_TIMESTAMP;
		
		String sql = "select first(s_0) from group_3 ";
		Query q = new Query(sql, config.INFLUX_DB_NAME);
		QueryResult results = influxDB.query(q);

		for (QueryResult.Result result : results.getResults()) {
			if (result.getSeries() == null) {
				break;
			}
			for (QueryResult.Series s : result.getSeries()) {
				for (List<Object> values : s.getValues()) {
					String str = values.get(0).toString();
					try {
						date = sdf.parse(str);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					startTime = date.getTime();
				}
			}
		} // for

		sql = "select last(s_0) from group_3 ";
		q = new Query(sql, config.INFLUX_DB_NAME);
		results = influxDB.query(q);
		for (QueryResult.Result result : results.getResults()) {
			if (result.getSeries() == null) {
				break;
			}
			for (QueryResult.Series s : result.getSeries()) {
				for (List<Object> values : s.getValues()) {
					String str = values.get(0).toString();
					try {
						date = sdf.parse(str);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					endTime = date.getTime();
				}
			}
		}
		LOGGER.info("总的时间间隔为：{}", (endTime - startTime));
		return (endTime - startTime);
	}

	@Override
	public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client,
			ThreadLocal<Long> errorCount) {
		String sql = "";
		long startTimeStamp = 0, endTimeStamp = 0;
		try {
			List<String> sensorList = new ArrayList<String>();
			switch (config.QUERY_CHOICE) {
			case 1:// 精确点查询
				long timeStamp = (startTime - Constants.START_TIMESTAMP) / config.POINT_STEP * config.POINT_STEP
						+ Constants.START_TIMESTAMP;
				if (config.IS_EMPTY_PRECISE_POINT_QUERY) {
					timeStamp += config.POINT_STEP / 2;
				}
				sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, timeStamp, sensorList);
				break;
			case 2:// 模糊点查询（暂未实现）
				sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, sensorList);
				break;
			case 3:// 聚合函数查询
				sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, config.QUERY_AGGREGATE_FUN, startTime,
						startTime + config.QUERY_INTERVAL, sensorList);
				break;
			case 4:// 范围查询
				sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, startTime,
						startTime + config.QUERY_INTERVAL, sensorList);
				break;
			case 5:// 条件查询
				sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, startTime,
						startTime + config.QUERY_INTERVAL, config.QUERY_LOWER_LIMIT, sensorList);
				break;
			case 6:// 最近点查询
				sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, "last", sensorList);
				break;
			case 7:// groupBy查询（暂时只有一个时间段）
				sql = createQuerySQLStatment(devices, config.QUERY_AGGREGATE_FUN, config.QUERY_SENSOR_NUM,
						startTime, startTime+config.QUERY_INTERVAL, config.QUERY_LOWER_LIMIT,
						sensorList);
				break;
			}
			int line = 0;
			StringBuilder builder = new StringBuilder(sql);
			startTimeStamp = System.currentTimeMillis();
			QueryResult results = influxDB.query(new Query(sql, config.INFLUX_DB_NAME));
			for (Result result : results.getResults()) {
				//LOGGER.info(result.toString());
				List<Series> series = result.getSeries();
				if (series == null) {
					break;
				}
				if (result.getError() != null) {
					errorCount.set(errorCount.get() + 1);
					LOGGER.error("{} execute query failed! Error：{}", Thread.currentThread().getName(),
							result.getError());
					LOGGER.error("执行失败的查询语句：{}", sql);
					mySql.saveQueryProcess(index, 0, (endTimeStamp - startTimeStamp) / 1000.0f, "query fail!" + sql);
				}
				for (Series serie : series) {
					List<List<Object>> values= serie.getValues();
					line += values.size()*(serie.getColumns().size()-1);
					//builder.append(serie.toString());
				}
			}

			//LOGGER.info("{}", builder.toString());
			endTimeStamp = System.currentTimeMillis();
			client.setTotalPoint(client.getTotalPoint() + line * config.QUERY_SENSOR_NUM);
			client.setTotalTime(client.getTotalTime() + endTimeStamp - startTimeStamp);

			LOGGER.info(
					"{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
							+ "TotalTime {}s with totalPoint {} rate is {}points/s",
					Thread.currentThread().getName(), index, (endTimeStamp - startTimeStamp) / 1000.0,
					line * config.QUERY_SENSOR_NUM,
					line * config.QUERY_SENSOR_NUM * 1000.0 / (endTimeStamp - startTimeStamp),
					(client.getTotalTime()) / 1000.0, client.getTotalPoint(),
					client.getTotalPoint() * 1000.0f / client.getTotalTime());
			mySql.saveQueryProcess(index, line * config.QUERY_SENSOR_NUM, (endTimeStamp - startTimeStamp) / 1000.0f,
					config.REMARK);
		} catch (SQLException e) {
			errorCount.set(errorCount.get() + 1);
			LOGGER.error("{} execute query failed! Error：{}", Thread.currentThread().getName(), e.getMessage());
			LOGGER.error("执行失败的查询语句：{}", sql);
			mySql.saveQueryProcess(index, 0, (endTimeStamp - startTimeStamp) / 1000.0f, "query fail!" + sql);
			e.printStackTrace();
		}

	}

	@Override
	public long count(String group, String device, String sensor) {
		String sql = String.format(countSQL, sensor, group, device);
		Query q = new Query(sql, config.INFLUX_DB_NAME);
		QueryResult results = influxDB.query(q);

		long countResult = 0;

		if (results.getResults() != null) {
			for (QueryResult.Result result : results.getResults()) {
				for (QueryResult.Series s : result.getSeries()) {
					for (List<Object> values : s.getValues()) {
						countResult = (long) Float.parseFloat(values.get(1).toString());
					}
				}
			}
		}
		return countResult;

	}

	/**
	 * 创建查询语句--(精确点查询)
	 * 
	 * @throws SQLException
	 */
	private String createQuerySQLStatment(List<Integer> devices, int num, long time, List<String> sensorList)
			throws SQLException {
		StringBuilder builder = new StringBuilder(createQuerySQLStatment(devices, num, sensorList));
		builder.append(" AND time = ").append(time * 1000000);
		return builder.toString();
	}

	public String getSelectClause(int num, String method, boolean is_aggregate_fun, List<String> sensorList)
			throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT ");
		if (num > config.SENSOR_NUMBER) {
			throw new SQLException("config.SENSOR_NUMBER is " + config.SENSOR_NUMBER
					+ " shouldn't less than the number of fields in querySql");
		}
		List<String> list = new ArrayList<String>();
		for (String sensor : config.SENSOR_CODES) {
			list.add(sensor);
		}
		Collections.shuffle(list, sensorRandom);
		if (is_aggregate_fun && method.length() > 2) {
			builder.append(method).append("(").append(list.get(0)).append(")");
			sensorList.add(list.get(0));
			for (int i = 1; i < num; i++) {
				builder.append(" , ").append(method).append("(").append(list.get(i)).append(")");
				sensorList.add(list.get(i));
			}
		} else {
			builder.append(list.get(0));
			sensorList.add(list.get(0));
			for (int i = 1; i < num; i++) {
				builder.append(" , ").append(list.get(i));
				sensorList.add(list.get(i));
			}
		}
		return builder.toString();
	}

	public String getPath(List<Integer> devices) {
		StringBuilder builder = new StringBuilder();
		Set<Integer> groups = new HashSet<>();
		for (int d : devices) {
			groups.add(d / config.GROUP_NUMBER);
		}
		builder.append(" FROM ");
		for (int g : groups) {
			builder.append(" group_" + g).append(" , ");
		}
		builder.deleteCharAt(builder.lastIndexOf(","));
		builder.append(" WHERE (");
		for (int d : devices) {
			builder.append(" device = 'd_" + d + "' OR");
		}
		builder.delete(builder.lastIndexOf("OR"), builder.length());
		builder.append(")");
		return builder.toString();
	}

	/**
	 * 创建查询语句--(查询设备下的num个传感器数值)
	 * 
	 * @throws SQLException
	 */
	// select s_0, s_1 from group_0,group_1;
	private String createQuerySQLStatment(List<Integer> devices, int num, List<String> sensorList) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(getSelectClause(num, "", false, sensorList));
		builder.append(getPath(devices));
		return builder.toString();
	}

	/**
	 * 创建查询语句--(带有聚合函数的查询)
	 * 
	 * @throws SQLException
	 */
	private String createQuerySQLStatment(List<Integer> devices, int num, String method, List<String> sensorList)
			throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(getSelectClause(num, method, true, sensorList));
		builder.append(getPath(devices));
		return builder.toString();
	}

	/**
	 * 创建查询语句--(带有聚合函数以及时间范围的查询)
	 * 
	 * @throws SQLException
	 */
	private String createQuerySQLStatment(List<Integer> devices, int num, String method, long startTime, long endTime,
			List<String> sensorList) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(createQuerySQLStatment(devices, num, method, sensorList));
		builder.append(" AND time > ");
		builder.append(startTime * 1000000).append(" AND time < ").append(endTime * 1000000);
		return builder.toString();
	}

	/**
	 * 创建查询语句--(带有时间约束条件的查询)
	 * 
	 * @throws SQLException
	 */
	private String createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime,
			List<String> sensorList) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(createQuerySQLStatment(devices, num, sensorList)).append(" AND time > ");
		builder.append(startTime * 1000000).append(" AND time < ").append(endTime * 1000000);
		return builder.toString();
	}

	/**
	 * 创建查询语句--(带有时间约束以及条件约束的查询)
	 * 
	 * @throws SQLException
	 */
	private String createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime, Number value,
			List<String> sensorList) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(createQuerySQLStatment(devices, num, startTime, endTime, sensorList));

		for (int i = 0; i < sensorList.size(); i++) {
			builder.append(" AND ").append(sensorList.get(i)).append(" > ").append(value);
		}

		return builder.toString();
	}

	/**
	 * 创建查询语句--(带有时间约束以及条件约束的GroupBy查询)
	 *
	 * @throws SQLException
	 */
	private String createQuerySQLStatment(List<Integer> devices, String method, int num, long startTime, long endTime,
			Number value, List<String> sensorList) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(createQuerySQLStatment(devices, num, method, startTime, endTime, sensorList));

		for (int i = 0; i < sensorList.size(); i++) {
			builder.append(" AND ").append(sensorList.get(i)).append(" > ").append(value);
		}
		builder.append(" GROUP BY time(").append(config.TIME_UNIT).append("ms)");
		return builder.toString();
	}
    public void flush(){

    }

    @Override
    public void getUnitPointStorageSize() throws SQLException {

    }

    @Override
    public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount){

    }

    @Override
    public void createSchemaOfDataGen() throws SQLException {

    }

    @Override
    public void insertGenDataOneBatch(String s, int i, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

    }

	@Override
	public void exeSQLFromFileByOneBatch() {

	}

//    /**
//	 * 创建查询语句--(精确点查询)
//	 * 
//	 * @throws SQLException
//	 */
//	private String createQuerySQLStatment(List<Integer> devices, int num, long time, List<String> sensorList)
//			throws SQLException {
//		StringBuilder builder = new StringBuilder(createQuerySQLStatment(devices, num, sensorList));
//		builder.append(" WHERE time = ").append(time);
//		return builder.toString();
//	}
//
//	/**
//	 * 创建查询语句--(查询设备下的num个传感器数值)
//	 * 
//	 * @throws SQLException
//	 */
//	private String createQuerySQLStatment(List<Integer> devices, int num, List<String> sensorList) throws SQLException {
//		StringBuilder builder = new StringBuilder();
//		builder.append("SELECT ");
//		if (num > config.SENSOR_NUMBER) {
//			throw new SQLException("config.SENSOR_NUMBER is " + config.SENSOR_NUMBER
//					+ " shouldn't less than the number of fields in querySql");
//		}
//		List<String> list = new ArrayList<String>();
//		for (String sensor : config.SENSOR_CODES) {
//			list.add(sensor);
//		}
//		Collections.shuffle(list);
//		builder.append(list.get(0));
//		sensorList.add(list.get(0));
//		for (int i = 1; i < num; i++) {
//			builder.append(" , ").append(list.get(i));
//			sensorList.add(list.get(i));
//		}
//		builder.append(" FROM ").append(getFullGroupDevicePathByID(devices.get(0)));
//		for (int i = 1; i < devices.size(); i++) {
//			builder.append(" , ").append(getFullGroupDevicePathByID(devices.get(i)));
//		}
//
//		return builder.toString();
//	}
//
//	/** 创建查询语句--(带有聚合函数的查询) */
//	private String createQuerySQLStatment(List<Integer> devices, int num, String method, List<String> sensorList) {
//		StringBuilder builder = new StringBuilder();
//
//		builder.append("SELECT ");
//
//		List<String> list = new ArrayList<String>();
//		for (String sensor : config.SENSOR_CODES) {
//			list.add(sensor);
//		}
//		Collections.shuffle(list);
//		if(method.length()>2) {
//			builder.append(method).append("(").append(list.get(0)).append(")");
//			sensorList.add(list.get(0));
//			for (int i = 1; i < num; i++) {
//				builder.append(" , ").append(method).append("(").append(list.get(i)).append(")");
//				sensorList.add(list.get(i));
//			}
//		}
//		else {
//			builder.append(list.get(0));
//			sensorList.add(list.get(0));
//			for (int i = 1; i < num; i++) {
//				builder.append(" , ").append(list.get(i));
//				sensorList.add(list.get(i));
//			}
//		}
//		
//
//		builder.append(" FROM ").append(getFullGroupDevicePathByID(devices.get(0)));
//		for (int i = 1; i < devices.size(); i++) {
//			builder.append(" , ").append(getFullGroupDevicePathByID(devices.get(i)));
//		}
//		return builder.toString();
//	}
//
//	/**
//	 * 创建查询语句--(带有时间约束条件的查询)
//	 * 
//	 * @throws SQLException
//	 */
//	private String createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime,
//			List<String> sensorList) throws SQLException {
//		StringBuilder builder = new StringBuilder();
//		builder.append(createQuerySQLStatment(devices, num, sensorList)).append(" WHERE time > ");
//		builder.append(startTime).append(" AND time < ").append(endTime);
//		return builder.toString();
//	}
//
//	/**
//	 * 创建查询语句--(带有时间约束以及条件约束的查询)
//	 * 
//	 * @throws SQLException
//	 */
//	private String createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime, Number value,
//			List<String> sensorList) throws SQLException {
//		StringBuilder builder = new StringBuilder();
//		builder.append(createQuerySQLStatment(devices, num, startTime, endTime, sensorList));
//		
//		for (int id : devices) {
//			String prefix = getFullGroupDevicePathByID(id);
//			for (int i = 0; i < sensorList.size(); i++) {
//				builder.append(" AND ").append(prefix).append(".").append(sensorList.get(i)).append(" > ")
//				.append(value);
//				
//			}
//		}
//		
//		return builder.toString();
//	}
//	
//	/**
//	 * 创建查询语句--(带有时间约束以及条件约束的GroupBy查询)
//	 * 
//	 * @throws SQLException
//	 */
//	private String createQuerySQLStatment(List<Integer> devices, String method, int num, List<Long> startTime, List<Long> endTime, Number value,
//			List<String> sensorList) throws SQLException {
//		StringBuilder builder = new StringBuilder();
//		builder.append(createQuerySQLStatment(devices,num,method, sensorList));
//		builder.append(" WHERE ");
//		for (int id : devices) {
//			String prefix = getFullGroupDevicePathByID(id);
//			for (int i = 0; i < sensorList.size(); i++) {
//				builder.append(prefix).append(".").append(sensorList.get(i)).append(" > ")
//				.append(value).append(" AND ");
//			}
//		}
//		builder.delete(builder.lastIndexOf("AND"), builder.length());
//		builder.append(" GROUP BY(").append(config.QUERY_INTERVAL).append("ms, ").append(Constants.START_TIMESTAMP);
//		for(int i = 0;i<startTime.size();i++) {
//			builder.append(",[").append(startTime.get(i)).append(",").append(endTime.get(i)).append("]");
//		}
//		builder.append(")");
//		return builder.toString();
//	}

}
