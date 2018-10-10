package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.model.InfluxDataModel;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * This version use influx-java api instead of simple http.
 */
public class InfluxDBV2 implements IDatebase {
	private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBV2.class);

	private String InfluxURL;
	private String InfluxDBName;
	private static Config config;
	private final String DEFAULT_RP = "autogen";
	private static final String countSQL = "select count(%s) from %s where device='%s'";
	private org.influxdb.InfluxDB influxDB;
	private MySqlLog mySql;
	private long labID;
	private Random sensorRandom = null;
	private Random timestampRandom;
	private ProbTool probTool;

	public InfluxDBV2(long labID) {
		config = ConfigDescriptor.getInstance().getConfig();
		mySql = new MySqlLog();
		this.labID = labID;
		probTool = new ProbTool();
		timestampRandom = new Random(2 + config.QUERY_SEED);
	}

	@Override
	public void init() throws SQLException {
		InfluxURL = config.DB_URL;
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
			startTime = System.nanoTime();
			influxDB.write(batchPoints);
			endTime = System.nanoTime();
			LOGGER.info("{} execute {} batch, it costs {}s, totalTime{}, throughput {} points/s",
					Thread.currentThread().getName(),
					batchIndex,
					(endTime - startTime) / 1000000000.0,
					((totalTime.get() + (endTime - startTime)) / 1000000000.0),
					config.SENSOR_NUMBER * (batchPoints.getPoints().size() / (double) (endTime - startTime)) * 1000000000.0);
			totalTime.set(totalTime.get() + (endTime - startTime));
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0, 0,
					config.REMARK);
		} catch (Exception e) {
			errorCount.set(errorCount.get() + batchPoints.getPoints().size());
			LOGGER.error("Batch insert failed, the failed num is {}! Error：{}", batchPoints.getPoints().size(),
					e.getMessage());
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0,
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
			startTime = System.nanoTime();
			influxDB.write(cons);
			endTime = System.nanoTime();
			LOGGER.info("{} execute {} batch, it costs {}s, totalTime{}, throughput {} items/s",
					Thread.currentThread().getName(),
					batchIndex,
					(endTime - startTime) / 1000000000.0,
					((totalTime.get() + (endTime - startTime)) / 1000000000.0),
					(cons.size() / (double) (endTime - startTime)) * 1000000000.0);
			totalTime.set(totalTime.get() + (endTime - startTime));
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0, 0,
					config.REMARK);
		} catch (Exception e) {
			errorCount.set(errorCount.get() + cons.size());
			LOGGER.error("Batch insert failed, the failed num is {}! Error：{}", cons.size(), e.getMessage());
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0, cons.size(),
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

	private InfluxDataModel createDataModel(int timestampIndex, String device) {
		InfluxDataModel model = new InfluxDataModel();
		int deviceNum = getDeviceNum(device);
		int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
		int groupNum = deviceNum / groupSize;
		model.measurement = "group_" + groupNum;
		model.tagSet.put("device", device);
		long currentTime;
		if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
			currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * timestampIndex + (long) (config.POINT_STEP * timestampRandom.nextDouble());
		} else {
			currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * timestampIndex;
		}
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
							startTime + config.QUERY_INTERVAL, sensorList, true);
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
					sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, "last", sensorList, true);
					break;
				case 7:// groupBy查询（暂时只有一个时间段）
					sql = createQuerySQLStatment(devices, config.QUERY_AGGREGATE_FUN, config.QUERY_SENSOR_NUM,
							startTime, startTime + config.QUERY_INTERVAL, config.QUERY_LOWER_LIMIT,
							sensorList);
					break;
				case 8:// query with limit and series limit and their offsets
					int device_id = index % devices.size();
					sql = createQuerySQLStatment(device_id, config.QUERY_LIMIT_N, config.QUERY_LIMIT_OFFSET, config.QUERY_SLIMIT_N, config.QUERY_SLIMIT_OFFSET);
					break;
				case 9:// criteria query with limit
					sql = createQuerySQLStatment(
							devices,
							config.QUERY_SENSOR_NUM,
							startTime,
							startTime + config.QUERY_INTERVAL,
							config.QUERY_LOWER_LIMIT,
							sensorList,
							config.QUERY_LIMIT_N,
							config.QUERY_LIMIT_OFFSET);
					break;
				case 10:// aggregation function query without any filter
					sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, config.QUERY_AGGREGATE_FUN, sensorList, true);
					break;
				case 11:// aggregation function query with value filter
					sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, config.QUERY_AGGREGATE_FUN, config.QUERY_LOWER_LIMIT,
							sensorList);
					break;

			}
			LOGGER.debug(sql);
			int line = 0;
			StringBuilder builder = new StringBuilder(sql);
			LOGGER.info("{} execute {} loop,提交执行的sql：{}", Thread.currentThread().getName(), index, builder.toString());
			startTimeStamp = System.nanoTime();
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
					mySql.saveQueryProcess(index, 0, (endTimeStamp - startTimeStamp) / 1000000000.0f, "query fail!" + sql);
				}
				for (Series serie : series) {
					List<List<Object>> values= serie.getValues();
					line += values.size()*(serie.getColumns().size()-1);
					//builder.append(serie.toString());
				}
			}

			//LOGGER.info("{}", builder.toString());
			endTimeStamp = System.nanoTime();
			client.setTotalPoint(client.getTotalPoint() + line * config.QUERY_SENSOR_NUM);
			client.setTotalTime(client.getTotalTime() + endTimeStamp - startTimeStamp);

			LOGGER.info(
					"{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
							+ "TotalTime {}s with totalPoint {} rate is {}points/s",
					Thread.currentThread().getName(), index, ((endTimeStamp - startTimeStamp) / 1000.0f)/1000000.0,
//					line * config.QUERY_SENSOR_NUM,
//					line * config.QUERY_SENSOR_NUM * 1000.0 / ((endTimeStamp - startTimeStamp)/1000000.0),
					// FIXME
					line ,
					line * 1000.0 / ((endTimeStamp - startTimeStamp)/1000000.0),
					((client.getTotalTime()) / 1000.0)/1000000.0, client.getTotalPoint(),
					client.getTotalPoint() * 1000.0f / (client.getTotalTime()/1000000.0));
			mySql.saveQueryProcess(index, line * config.QUERY_SENSOR_NUM, ((endTimeStamp - startTimeStamp) / 1000.0f)/1000000.0,
					config.REMARK);
		} catch (SQLException e) {
			errorCount.set(errorCount.get() + 1);
			LOGGER.error("{} execute query failed! Error：{}", Thread.currentThread().getName(), e.getMessage());
			LOGGER.error("执行失败的查询语句：{}", sql);
			mySql.saveQueryProcess(index, 0, ((endTimeStamp - startTimeStamp) / 1000.0f)/1000000.0, "query fail!" + sql);
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
	 * 创建查询语句--(带有limit条件的条件查询)
	 *
	 * @throws SQLException
	 */
	public String createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime, Number value,
										 List<String> sensorList, int limit_n, int offset) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(createQuerySQLStatment(devices, num, startTime, endTime, value, sensorList)).append(" LIMIT ").append(limit_n);
		builder.append(" OFFSET ").append(offset);
		return builder.toString();
	}

	/**
	 * 创建查询语句--(带有limit条件的查询)
	 *
	 * @throws SQLException
	 */
	public String createQuerySQLStatment(int device_id, int limit_n, int offset, int series_limit, int series_offset) {
		StringBuilder builder = new StringBuilder();
		List<Integer> devices = new ArrayList<>();
		devices.add(device_id);
		builder.append("SELECT * ");
		builder.append(getPath(devices));
		builder.append(" LIMIT ").append(limit_n).append(" OFFSET ").append(offset);
		builder.append(" SLIMIT ").append(series_limit).append(" SOFFSET ").append(series_offset);
		return builder.toString();
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
			groups.add(d / (config.DEVICE_NUMBER/config.GROUP_NUMBER));
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
	private String createQuerySQLStatment(List<Integer> devices, int num, String method, List<String> sensorList, boolean isFinal)
			throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(getSelectClause(num, method, true, sensorList));
		builder.append(getPath(devices));
		if(isFinal) {
			builder.append(" group by device");
		}
		return builder.toString();
	}

	/**
	 * 创建查询语句--(带有聚合函数以及时间范围的查询)
	 * 
	 * @throws SQLException
	 */
	private String createQuerySQLStatment(List<Integer> devices, int num, String method, long startTime, long endTime,
			List<String> sensorList, boolean flag) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(createQuerySQLStatment(devices, num, method, sensorList, false));
		builder.append(" AND time > ");
		builder.append(startTime * 1000000).append(" AND time < ").append(endTime * 1000000);
		if(flag) {
			builder.append(" group by device");
		}
		return builder.toString();
	}

	/**
	 * 创建查询语句--(带有value约束的聚合查询)
	 *
	 * @throws SQLException
	 */
	public String createQuerySQLStatment(List<Integer> devices, int num, String method, Number value,
										 List<String> sensorList) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(createQuerySQLStatment(devices, num, method, sensorList, false));
		for (int i = 0; i < sensorList.size(); i++) {
			builder.append(" AND ").append(sensorList.get(i)).append(" > ").append(value);
		}
		builder.append(" group by device");
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
		builder.append(" group by device");
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
		builder.delete(builder.lastIndexOf("group by device"), builder.length());

		for (int i = 0; i < sensorList.size(); i++) {
			builder.append(" AND ").append(sensorList.get(i)).append(" > ").append(value);
		}
		builder.append(" group by device");

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
		builder.append(createQuerySQLStatment(devices, num, method, startTime, endTime, sensorList, false));

		for (int i = 0; i < sensorList.size(); i++) {
			builder.append(" AND ").append(sensorList.get(i)).append(" > ").append(value);
		}
		builder.append(" GROUP BY time(").append(config.TIME_UNIT).append("ms)");
		return builder.toString();
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

	@Override
	public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex, Random random) throws SQLException {
		return 0;
	}

	@Override
	public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random) throws SQLException {
		int timestampIndex;
		PossionDistribution possionDistribution = new PossionDistribution(random);
		int nextDelta;

		BatchPoints batchPoints = BatchPoints.database(InfluxDBName).tag("async", "true").retentionPolicy(DEFAULT_RP)
				.consistency(org.influxdb.InfluxDB.ConsistencyLevel.ALL).build();

		if (loopIndex == 0) {
			InfluxDataModel model = createDataModel(maxTimestampIndex, device);
			batchPoints.point(model.toInfluxPoint());
			for (int i = 1; i < config.CACHE_NUM; i++) {
				if (probTool.returnTrueByProb(1.0 - config.OVERFLOW_RATIO, random)) {
					maxTimestampIndex++;
					timestampIndex = maxTimestampIndex;
				} else {
					nextDelta = possionDistribution.getNextPossionDelta();
					timestampIndex = maxTimestampIndex - nextDelta;
				}
				System.out.println("timestampIndex:" + timestampIndex);
				model = createDataModel(timestampIndex, device);
				batchPoints.point(model.toInfluxPoint());

			}
		}else {
			InfluxDataModel model;
			for (int i = 0; i < config.CACHE_NUM; i++) {
				if (probTool.returnTrueByProb(1.0 - config.OVERFLOW_RATIO, random)) {
					maxTimestampIndex++;
					timestampIndex = maxTimestampIndex;
				} else {
					nextDelta = possionDistribution.getNextPossionDelta();
					timestampIndex = maxTimestampIndex - nextDelta;
				}
				System.out.println("timestampIndex:" + timestampIndex);
				model = createDataModel(timestampIndex, device);
				batchPoints.point(model.toInfluxPoint());
			}
		}
		long startTime = 0, endTime = 0;
		try {
			startTime = System.nanoTime();
			influxDB.write(batchPoints);
			endTime = System.nanoTime();
			LOGGER.info("{} execute {} batch, it costs {}s, totalTime{}, throughput {} points/s",
					Thread.currentThread().getName(),
					loopIndex,
					(endTime - startTime) / 1000000000.0,
					((totalTime.get() + (endTime - startTime)) / 1000000000.0),
					config.SENSOR_NUMBER * (batchPoints.getPoints().size() / (double) (endTime - startTime)) * 1000000000.0);
			totalTime.set(totalTime.get() + (endTime - startTime));
			mySql.saveInsertProcess(loopIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0, 0,
					config.REMARK);
		} catch (Exception e) {
			errorCount.set(errorCount.get() + batchPoints.getPoints().size());
			LOGGER.error("Batch insert failed, the failed num is {}! Error：{}", batchPoints.getPoints().size(),
					e.getMessage());
			mySql.saveInsertProcess(loopIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0,
					batchPoints.getPoints().size(), config.REMARK);
			//throw new SQLException(e.getMessage());
		}


		return maxTimestampIndex;
	}

	static public void main(String[] args) throws SQLException {
//		InfluxDBV2 influxDB = new InfluxDBV2(0);
//		influxDB.init();
//		ThreadLocal<Long> time = new ThreadLocal<>();
//		time.set((long) 0);
//		ThreadLocal<Long> errorCount = new ThreadLocal<>();
//		errorCount.set((long) 0);
//		//influxDB.insertOneBatch("D_0", 0, time, errorCount);
//		int maxTimestampIndex = 50;
//		Random random = new Random(0);
//		influxDB.insertOverflowOneBatchDist("D_0",2, time, errorCount, maxTimestampIndex, random);
		String sql ;
		InfluxDBV2 influxDB = new InfluxDBV2(0);
		List<Integer> devices;
		List<String> sensorList;
		devices = new ArrayList<>();
		sensorList = new ArrayList<>();
		devices.add(0);
		devices.add(210);
		config.DEVICE_NUMBER = 5000;
		config.GROUP_NUMBER = 50;
//		sql = influxDB.createQuerySQLStatment(devices, 3, "max", 0,
//				100000000 + 25000, sensorList);
		sql = influxDB.createQuerySQLStatment(devices, 3, "max",  sensorList, true);
		System.out.println(sql);
	}

	@Override
	public long getLabID(){
		return labID;
	}

}
