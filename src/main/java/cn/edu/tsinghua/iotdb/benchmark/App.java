package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.*;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.*;
import cn.edu.tsinghua.iotdb.benchmark.tool.*;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Resolve;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Storage;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;

public class App {
	private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		CommandCli cli = new CommandCli();
		if (!cli.init(args)) {
			return;
		}
		Config config = ConfigDescriptor.getInstance().getConfig();
		switch (config.BENCHMARK_WORK_MODE) {
		case Constants.MODE_SERVER_MODE:
			serverMode(config);
			break;
		case Constants.MODE_INSERT_TEST_WITH_DEFAULT_PATH:
			insertTest(config);
			break;
		case Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH:
			genData(config);
			break;
		case Constants.MODE_QUERY_TEST_WITH_DEFAULT_PATH:
			queryTest(config);
			break;
		case Constants.MODE_IMPORT_DATA_FROM_CSV:
			importDataFromCSV(config);
			break;
		default:
			throw new SQLException("unsupported mode " + config.BENCHMARK_WORK_MODE);
	}
		
	}// main
	
	/**
	 * 将数据从CSV文件导入IOTDB
	 * @throws SQLException 
	 * 
	 * */
	private static void importDataFromCSV(Config config) throws SQLException {
		// TODO Auto-generated method stub
		MetaDateBuilder builder = new MetaDateBuilder();
		builder.createMataData(config.METADATA_FILE_PATH);
		ImportDataFromCSV importTool = new ImportDataFromCSV();
		importTool.importData(config.IMPORT_DATA_FILE_PATH);
	}

	/**
	 * 服务器端模式，监测系统内存等性能指标，获得插入的数据文件大小
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private static void serverMode(Config config) throws SQLException, ClassNotFoundException{
		MySqlLog mySql = new MySqlLog();
		mySql.initMysql(System.currentTimeMillis());
		File dir = new File(config.LOG_STOP_FLAG_PATH);
		if (dir.exists() && dir.isDirectory()) {
			File file = new File(config.LOG_STOP_FLAG_PATH + "/log_stop_flag");
			int interval = config.INTERVAL;
			// 检测所需的时间在目前代码的参数下至少为2秒
			LOGGER.info("----------New Test Begin with interval about {} s----------", interval + 2);
			while (true) {
				ArrayList<Float> ioUsageList = IoUsage.getInstance().get();
				ArrayList<Float> netUsageList = NetUsage.getInstance().get();
				LOGGER.info("CPU使用率,{}", ioUsageList.get(0));
				LOGGER.info("内存使用率,{}", MemUsage.getInstance().get());
				LOGGER.info("磁盘IO使用率,{}", ioUsageList.get(1));
				LOGGER.info("eth0接收和发送速率,{},{},KB/s", netUsageList.get(0), netUsageList.get(1));
				mySql.insertSERVER_MODE(ioUsageList.get(0), MemUsage.getInstance().get(), ioUsageList.get(1),
						netUsageList.get(0), netUsageList.get(1),"");
				try {
					Thread.sleep(interval * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (file.exists()) {
					boolean f = file.delete();
					if (!f) {
						LOGGER.error("log_stop_flag 文件删除失败");
					}
					break;
				}
			}

			MySqlLog mysql = new MySqlLog();
			mysql.initMysql(System.currentTimeMillis());
			IDBFactory idbFactory = null;
			idbFactory = getDBFactory(config);

			IDatebase datebase;

			try {
				datebase = idbFactory.buildDB(mysql.getLabID());
				datebase.init();
				LOGGER.info("Before flush:");
				datebase.getUnitPointStorageSize();
				datebase.flush();
				LOGGER.info("After flush:");
				datebase.getUnitPointStorageSize();
				datebase.close();

			} catch (SQLException e) {
				LOGGER.error("Fail to init database becasue {}", e.getMessage());
				return;
			}


			mySql.closeMysql();




		} else {
			LOGGER.error("LOG_STOP_FLAG_PATH not exist!");
		}
	}
	

	private static void genData(Config config) throws SQLException, ClassNotFoundException  {
		//一次生成一个timeseries的数据
		MySqlLog mysql = new MySqlLog();
		mysql.initMysql(System.currentTimeMillis());
		mysql.saveTestModel(config.TIMESERIES_TYPE, config.ENCODING);
		mysql.savaTestConfig();
		IDBFactory idbFactory = null;
		idbFactory = getDBFactory(config);

		IDatebase datebase;
		long createSchemaStartTime;
		long createSchemaEndTime;
		float createSchemaTime;
		try {
			datebase = idbFactory.buildDB(mysql.getLabID());
			datebase.init();
			createSchemaStartTime = System.currentTimeMillis();
			datebase.createSchemaOfDataGen();
			datebase.close();
			createSchemaEndTime = System.currentTimeMillis();
			createSchemaTime = (createSchemaEndTime - createSchemaStartTime) / 1000.0f;
		} catch (SQLException e) {
			LOGGER.error("Fail to init database becasue {}", e.getMessage());
			return;
		}

		ArrayList<Long> totalInsertErrorNums = new ArrayList<>();
		long totalErrorPoint ;

			CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
			ArrayList<Long> totalTimes = new ArrayList<>();
			ExecutorService executorService = Executors
					.newFixedThreadPool(config.CLIENT_NUMBER);
			for (int i = 0; i < config.CLIENT_NUMBER; i++) {
				executorService.submit(new ClientThread(idbFactory.buildDB(mysql.getLabID()),
						i, downLatch, totalTimes, totalInsertErrorNums));
			}
			executorService.shutdown();
			try {
				downLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			long totalTime = 0;
			for (long c : totalTimes) {
				if (c > totalTime) {
					totalTime = c;
				}
			}
			long totalPoints = config.LOOP * config.CACHE_NUM;
			if(config.DB_SWITCH.equals(Constants.DB_IOT)&&config.MUL_DEV_BATCH){
				totalPoints = config.SENSOR_NUMBER * config.CLIENT_NUMBER * config.LOOP * config.CACHE_NUM ;
			}
			switch (config.DB_SWITCH) {
				case Constants.DB_IOT:
					totalErrorPoint = getErrorNumIoT(totalInsertErrorNums);
					break;
				case Constants.DB_INFLUX:
					totalErrorPoint = getErrorNumInflux(config, datebase);
					break;
				default:
					throw new SQLException("unsupported database " + config.DB_SWITCH);
			}
			LOGGER.info(
					"GROUP_NUMBER = ,{}, DEVICE_NUMBER = ,{}, SENSOR_NUMBER = ,{}, CACHE_NUM = ,{}, POINT_STEP = ,{}, LOOP = ,{}, MUL_DEV_BATCH = ,{}",
					config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER,
					config.CACHE_NUM, config.POINT_STEP,
					config.LOOP, config.MUL_DEV_BATCH);

			LOGGER.info(
					"Loaded ,{}, points in ,{},s with ,{}, workers (mean rate ,{}, points/s)",
					totalPoints ,
					totalTime / 1000.0f,
					config.CLIENT_NUMBER,
					1000.0f * (totalPoints - totalErrorPoint) / (float) totalTime);

			LOGGER.info("Total error num is {}, create schema cost {},s",
					totalErrorPoint, createSchemaTime);


			//加入新版的mysql表中
			mysql.saveResult("createSchemaTime(s)", ""+createSchemaTime);
			mysql.saveResult("totalPoints", ""+totalPoints);
			mysql.saveResult("totalTime(s)", ""+totalTime / 1000.0f);
			mysql.saveResult("totalErrorPoint", ""+totalErrorPoint);
			mysql.closeMysql();

	}


	/**
	 * 数据库插入测试
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private static void insertTest(Config config) throws SQLException,
			ClassNotFoundException {
		MySqlLog mysql = new MySqlLog();
		mysql.initMysql(System.currentTimeMillis());
		mysql.saveTestModel("Double", config.ENCODING);
		mysql.savaTestConfig();
		
		IDBFactory idbFactory = null;
		idbFactory = getDBFactory(config);

		IDatebase datebase;
		long createSchemaStartTime;
		long createSchemaEndTime;
		float createSchemaTime;
		try {
			datebase = idbFactory.buildDB(mysql.getLabID());
			datebase.init();
			createSchemaStartTime = System.currentTimeMillis();
			datebase.createSchema();
			datebase.close();
			createSchemaEndTime = System.currentTimeMillis();
			createSchemaTime = (createSchemaEndTime - createSchemaStartTime) / 1000.0f;
		} catch (SQLException e) {
			LOGGER.error("Fail to init database becasue {}", e.getMessage());
			return;
		}

		ArrayList<Long> totalInsertErrorNums = new ArrayList<>();
		long totalErrorPoint ;
		if (config.READ_FROM_FILE) {
			CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
			ArrayList<Long> totalTimes = new ArrayList<>();
			Storage storage = new Storage();
			ExecutorService executorService = Executors
					.newFixedThreadPool(config.CLIENT_NUMBER + 1);
			executorService.submit(new Resolve(config.FILE_PATH, storage));
			for (int i = 0; i < config.CLIENT_NUMBER; i++) {
				executorService
						.submit(new ClientThread(idbFactory.buildDB(mysql.getLabID()), i,
								storage, downLatch, totalTimes,
								totalInsertErrorNums));
			}
			executorService.shutdown();
			// wait for all threads complete
			try {
				downLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			int totalItem = storage.getStoragedProductNum();
			long totalTime = 0;
			for (long c : totalTimes) {
				if (c > totalTime) {
					totalTime = c;
				}
			}
			LOGGER.info(
					"READ_FROM_FILE = true, TAG_PATH = ,{}, STORE_MODE = ,{}, BATCH_OP_NUM = ,{}",
					config.TAG_PATH, config.STORE_MODE, config.BATCH_OP_NUM);
			LOGGER.info(
					"loaded ,{}, items in ,{},s with ,{}, workers (mean rate ,{}, items/s)",
					totalItem, totalTime / 1000.0f, config.CLIENT_NUMBER,
					(1000.0f * totalItem) / ((float) totalTime));

		} else {
			CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
			ArrayList<Long> totalTimes = new ArrayList<>();
			ExecutorService executorService = Executors
					.newFixedThreadPool(config.CLIENT_NUMBER);
			for (int i = 0; i < config.CLIENT_NUMBER; i++) {
				executorService.submit(new ClientThread(idbFactory.buildDB(mysql.getLabID()),
						i, downLatch, totalTimes, totalInsertErrorNums));
			}
			executorService.shutdown();
			try {
				downLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			long totalTime = 0;
			for (long c : totalTimes) {
				if (c > totalTime) {
					totalTime = c;
				}
			}
			long totalPoints = config.SENSOR_NUMBER * config.DEVICE_NUMBER * config.LOOP * config.CACHE_NUM;
			if(config.DB_SWITCH.equals(Constants.DB_IOT)&&config.MUL_DEV_BATCH){
				totalPoints = config.SENSOR_NUMBER * config.CLIENT_NUMBER * config.LOOP * config.CACHE_NUM ;
			}
			switch (config.DB_SWITCH) {
				case Constants.DB_IOT:
					totalErrorPoint = getErrorNumIoT(totalInsertErrorNums);
					break;
				case Constants.DB_INFLUX:
					totalErrorPoint = getErrorNumInflux(config, datebase);
					break;
				default:
					throw new SQLException("unsupported database " + config.DB_SWITCH);
			}
			LOGGER.info(
					"GROUP_NUMBER = ,{}, DEVICE_NUMBER = ,{}, SENSOR_NUMBER = ,{}, CACHE_NUM = ,{}, POINT_STEP = ,{}, LOOP = ,{}, MUL_DEV_BATCH = ,{}",
					config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER,
					config.CACHE_NUM, config.POINT_STEP,
					config.LOOP, config.MUL_DEV_BATCH);

			LOGGER.info(
					"Loaded ,{}, points in ,{},s with ,{}, workers (mean rate ,{}, points/s)",
					totalPoints ,
					totalTime / 1000.0f,
					config.CLIENT_NUMBER,
					1000.0f * (totalPoints - totalErrorPoint) / (float) totalTime);

			LOGGER.info("Total error num is {}, create schema cost {},s",
						totalErrorPoint, createSchemaTime);

			mysql.saveResult("createSchemaTime(s)", ""+createSchemaTime);
			mysql.saveResult("totalPoints", ""+totalPoints);
			mysql.saveResult("totalTime(s)", ""+totalTime / 1000.0f);
			mysql.saveResult("totalErrorPoint", ""+totalErrorPoint);
			mysql.closeMysql();

		}// else--
		
		
	}

	private static IDBFactory getDBFactory(Config config) throws SQLException{
		switch (config.DB_SWITCH) {
			case Constants.DB_IOT:
				return new IoTDBFactory();
			case Constants.DB_INFLUX:
				return new InfluxDBFactory();
			default:
				throw new SQLException("unsupported database " + config.DB_SWITCH);
		}
	}

	private static long getErrorNumInflux(Config config, IDatebase database) {
		//同一个device中不同sensor的点数是相同的，因此不对sensor遍历
		long insertedPointNum = 0;
		int groupIndex = 0;
		int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
		for(int i=0;i<config.DEVICE_NUMBER;i++){
			groupIndex=i/groupSize;
			insertedPointNum += database.count("group_" + groupIndex,"d_" + i,"s_0") * config.SENSOR_NUMBER;
		}
		try {
			database.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return config.SENSOR_NUMBER * config.DEVICE_NUMBER * config.LOOP * config.CACHE_NUM - insertedPointNum;
	}

	private static long getErrorNumIoT(ArrayList<Long> totalInsertErrorNums) {
		return getSumOfList(totalInsertErrorNums);
	}

	/**
	 * 数据库查询测试
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private static void queryTest(Config config) throws SQLException, ClassNotFoundException {
		IDBFactory idbFactory = null;
		idbFactory = getDBFactory(config);

		IDatebase datebase = null;
		MySqlLog mySql = new MySqlLog();
		try {
			mySql.initMysql(System.currentTimeMillis());
			datebase = idbFactory.buildDB(mySql.getLabID());
			datebase.init();
		} catch (SQLException e) {
			LOGGER.error("Fail to connect to database becasue {}", e.getMessage());
			return;
		}
		mySql.saveTestModel("Double", config.ENCODING);
		mySql.savaTestConfig();

		CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
		ArrayList<Long> totalTimes = new ArrayList<>();
		ArrayList<Long> totalPoints = new ArrayList<>();
		ArrayList<Long> totalQueryErrorNums = new ArrayList<>();
		ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
		for (int i = 0; i < config.CLIENT_NUMBER; i++) {
			executorService.submit(new QueryClientThread(idbFactory.buildDB(mySql.getLabID()), i, downLatch, totalTimes, totalPoints,
					totalQueryErrorNums));
		}
		executorService.shutdown();
		try {
			downLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long totalTime = 0;
		for (long c : totalTimes) {
			if (c > totalTime) {
				totalTime = c;
			}
		}
		long totalResultPoint = getSumOfList(totalPoints);

		LOGGER.info(
				"execute query ,{}, times in ,{},s with ,{}, result points by ,{}, workers (mean rate ,{}, points/s)",
				config.CLIENT_NUMBER * config.LOOP, totalTime / 1000.0f, totalResultPoint, config.CLIENT_NUMBER,
				(1000.0f * totalResultPoint) / ((float) totalTime));

		long totalErrorPoint = getSumOfList(totalQueryErrorNums);
		LOGGER.info("total error num is {}", totalErrorPoint);
		
		mySql.saveResult("queryNumber", ""+config.CLIENT_NUMBER * config.LOOP);
		mySql.saveResult("totalPoint", ""+totalResultPoint);
		mySql.saveResult("totalTime(s)", ""+totalTime / 1000.0f);
		mySql.saveResult("resultPointPerSecond(points/s)", ""+(1000.0f * (totalResultPoint) )/  totalTime);
		mySql.saveResult("totalErrorQuery", ""+totalErrorPoint);
		mySql.closeMysql();
	}

	/** 计算list中所有元素的和 */
	private static long getSumOfList(ArrayList<Long> list) {
		long total = 0;
		for (long c : list) {
			total += c;
		}
		return total;
	}


}
