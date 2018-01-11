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
import java.util.HashMap;
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
	private static final Logger LOGGER_RESULT = LoggerFactory.getLogger(App.class);

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
	

	private static void executeSQLFromFile(Config config) throws SQLException, ClassNotFoundException{
		MySqlLog mysql = new MySqlLog();
		mysql.initMysql(System.currentTimeMillis());
		IDBFactory idbFactory = null;
		idbFactory = getDBFactory(config);
		IDatebase datebase;
		long exeSQLFromFileStartTime;
		long exeSQLFromFileEndTime;
		float exeSQLFromFileTime = 1;
		int SQLCount = 0;
		try {
			datebase = idbFactory.buildDB(mysql.getLabID());
			datebase.init();
			exeSQLFromFileStartTime = System.currentTimeMillis();
			SQLCount = datebase.exeSQLFromFileByOneBatch();
			datebase.close();
			exeSQLFromFileEndTime = System.currentTimeMillis();
			exeSQLFromFileTime = (exeSQLFromFileEndTime - exeSQLFromFileStartTime) / 1000.0f;
		} catch (SQLException e) {
			LOGGER.error("Fail to init database becasue {}", e.getMessage());
			return;
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOGGER.info("Execute SQL from file {} by one batch cost {} seconds. Mean rate {} SQL/s",
				config.SQL_FILE,
				exeSQLFromFileTime,
				1.0f * SQLCount / exeSQLFromFileTime
				);


		//加入新版的mysql表中
		mysql.closeMysql();
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

			totalErrorPoint = getErrorNum(config,totalInsertErrorNums,datebase);
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

			totalErrorPoint = getErrorNum(config,totalInsertErrorNums,datebase);
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

			LOGGER.info("Total error num is {}, create schema cost ,{},s",
						totalErrorPoint, createSchemaTime);

			/*
			LOGGER_RESULT.error(
					"Writing test parameters: GROUP_NUMBER=,{},DEVICE_NUMBER=,{},SENSOR_NUMBER=,{},CACHE_NUM=,{},POINT_STEP=,{},LOOP=,{},MUL_DEV_BATCH=,{},IS_OVERFLOW=,{}",
					config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER,
					config.CACHE_NUM, config.POINT_STEP,
					config.LOOP, config.MUL_DEV_BATCH, config.IS_OVERFLOW);
			*/

			LOGGER_RESULT.error(
					"Writing test parameters: GROUP_NUMBER={}, DEVICE_NUMBER={}, SENSOR_NUMBER={}, CACHE_NUM={}, POINT_STEP={}, LOOP={}, MUL_DEV_BATCH={}, IS_OVERFLOW={}",
					config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER,
					config.CACHE_NUM, config.POINT_STEP,
					config.LOOP, config.MUL_DEV_BATCH, config.IS_OVERFLOW);

			/*
			LOGGER_RESULT.error("Loaded,{},points in,{},seconds, mean rate,{},points/s, Total error point num is,{},create schema cost,{},seconds",
					totalPoints,
					totalTime / 1000.0f,
					1000.0f * (totalPoints - totalErrorPoint) / (float) totalTime,
					totalErrorPoint,
					createSchemaTime);
			*/

			HashMap<String,String> lastPeriodResults = getLastPeriodResults(config);
			File file = new File(config.LAST_RESULT_PATH + "/lastPeriodResult.txt");
			float lastRate = 1;
			if (file.exists()) {
				LOGGER_RESULT.error("Last period loaded {} points in {} seconds, mean rate {} points/s, total error point num is {} , create schema cost {} seconds.",
						lastPeriodResults.get("WriteTotalPoint"),
						lastPeriodResults.get("WriteTotalTime"),
						lastPeriodResults.get("WriteMeanRate"),
						lastPeriodResults.get("WriteErrorNum"),
						lastPeriodResults.get("WriteSchemaCost"));
				lastRate = Float.parseFloat(lastPeriodResults.get("WriteMeanRate"));
			}

			float thisRate = 1000.0f * (totalPoints - totalErrorPoint) / (float) totalTime;
			LOGGER_RESULT.error("This period loaded {} points in {} seconds, mean rate {} points/s, total error point num is {} , create schema cost {} seconds. Mean rate change {} %.",
					totalPoints,
					totalTime / 1000.0f,
					thisRate,
					totalErrorPoint,
					createSchemaTime,
					((thisRate - lastRate) / lastRate * 100)
			);


			mysql.saveResult("createSchemaTime(s)", ""+createSchemaTime);
			mysql.saveResult("totalPoints", ""+totalPoints);
			mysql.saveResult("totalTime(s)", ""+totalTime / 1000.0f);
			mysql.saveResult("totalErrorPoint", ""+totalErrorPoint);
			mysql.closeMysql();

		}// else--
		
		
	}


	private static long getErrorNum(Config config, ArrayList<Long> totalInsertErrorNums, IDatebase datebase) throws SQLException {
		long totalErrorPoint ;
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
		return totalErrorPoint;
  }
  
	private static HashMap<String,String> getLastPeriodResults(Config config) {
		File dir = new File(config.LAST_RESULT_PATH);
		HashMap<String,String> lastResults = new HashMap<>();
		if (dir.exists() && dir.isDirectory()) {
			File file = new File(config.LAST_RESULT_PATH + "/lastPeriodResult.txt");
			if (file.exists()) {
				BufferedReader br = null;
				try {
					br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				String line = null;


				try {
					while ((line = br.readLine()) != null) {
						String[] writeResult = line.split("\\s+");
						if (writeResult[0].startsWith("This")) {
							if (writeResult[2].startsWith("load")) {
								lastResults.put("WriteTotalPoint", writeResult[3]);
								lastResults.put("WriteTotalTime", writeResult[6]);
								lastResults.put("WriteMeanRate", writeResult[10]);
								lastResults.put("WriteErrorNum", writeResult[17]);
								lastResults.put("WriteSchemaCost", writeResult[22]);
							} else if (writeResult[2].startsWith("Exa")) {
								lastResults.put("ExaQueryNum", writeResult[6]);
								lastResults.put("ExaQueryTime", writeResult[9]);
								lastResults.put("ExaQueryResults", writeResult[12]);
								lastResults.put("ExaQueryWorkers", writeResult[16]);
								lastResults.put("ExaQueryRate", writeResult[20]);
								lastResults.put("ExaQueryPointRate", writeResult[23]);
								lastResults.put("ExaQueryErrorNum", writeResult[31]);
							} else if (writeResult[2].startsWith("Fuz")) {
								lastResults.put("FuzQueryNum", (writeResult[6]));
								lastResults.put("FuzQueryTime", (writeResult[9]));
								lastResults.put("FuzQueryResults", (writeResult[12]));
								lastResults.put("FuzQueryWorkers", (writeResult[16]));
								lastResults.put("FuzQueryRate", (writeResult[20]));
								lastResults.put("FuzQueryPointRate", (writeResult[23]));
								lastResults.put("FuzQueryErrorNum", (writeResult[31]));
							} else if (writeResult[2].startsWith("Agg")) {
								lastResults.put("AggQueryNum", (writeResult[6]));
								lastResults.put("AggQueryTime", (writeResult[9]));
								lastResults.put("AggQueryResults", (writeResult[12]));
								lastResults.put("AggQueryWorkers", (writeResult[16]));
								lastResults.put("AggQueryRate", (writeResult[20]));
								lastResults.put("AggQueryPointRate", (writeResult[23]));
								lastResults.put("AggQueryErrorNum", (writeResult[31]));
							} else if (writeResult[2].startsWith("Ran")) {
								lastResults.put("RanQueryNum", (writeResult[5]));
								lastResults.put("RanQueryTime", (writeResult[8]));
								lastResults.put("RanQueryResults", (writeResult[11]));
								lastResults.put("RanQueryWorkers", (writeResult[15]));
								lastResults.put("RanQueryRate", (writeResult[19]));
								lastResults.put("RanQueryPointRate", (writeResult[22]));
								lastResults.put("RanQueryErrorNum", (writeResult[30]));
							} else if (writeResult[2].startsWith("Cri")) {
								lastResults.put("CriQueryNum", (writeResult[5]));
								lastResults.put("CriQueryTime", (writeResult[8]));
								lastResults.put("CriQueryResults", (writeResult[11]));
								lastResults.put("CriQueryWorkers", (writeResult[15]));
								lastResults.put("CriQueryRate", (writeResult[19]));
								lastResults.put("CriQueryPointRate", (writeResult[22]));
								lastResults.put("CriQueryErrorNum", (writeResult[30]));
							} else if (writeResult[2].startsWith("Nea")) {
								lastResults.put("NeaQueryNum", (writeResult[6]));
								lastResults.put("NeaQueryTime", (writeResult[9]));
								lastResults.put("NeaQueryResults", (writeResult[12]));
								lastResults.put("NeaQueryWorkers", (writeResult[16]));
								lastResults.put("NeaQueryRate", (writeResult[20]));
								lastResults.put("NeaQueryPointRate", (writeResult[23]));
								lastResults.put("NeaQueryErrorNum",(writeResult[31]));
							} else if (writeResult[2].startsWith("Gro")) {
								lastResults.put("GroQueryNum", (writeResult[6]));
								lastResults.put("GroQueryTime", (writeResult[9]));
								lastResults.put("GroQueryResults", (writeResult[12]));
								lastResults.put("GroQueryWorkers", (writeResult[16]));
								lastResults.put("GroQueryRate", (writeResult[20]));
								lastResults.put("GroQueryPointRate", (writeResult[23]));
								lastResults.put("GroQueryErrorNum", (writeResult[31]));
							}
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}else{
				LOGGER.warn("上一次测试结果的对比文件不存在");
			}
		}else{
			LOGGER.warn("上一次测试结果的路径不存在");
		}
			/*以下代码功能在脚本中实现了
			//读取上一次的结果完毕，删除上一次的结果文件
			if (file.exists()) {
				boolean f = file.delete();
				if (!f) {
					LOGGER.error("上一次测试结果的对比文件删除失败");
				}
			} else {
				LOGGER.error("上一次测试结果的对比文件不存在");
			}
			//获取进程
			Runtime run = Runtime.getRuntime();
			Process process = null;
			//将本次产生的新结果作为下一次测试的上一次测试结果文件
			String command = "cp  " + config.LAST_RESULT_PATH + "/log_result_info.txt" + "  " + config.LAST_RESULT_PATH + "/lastPeriodResult.txt";
			System.out.println(command);
			//执行doc命令
			try {
				process = run.exec(command);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else{
			LOGGER.error("上一次测试结果的路径不存在");
		}
			*/

		return lastResults;

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
				"{}: execute ,{}, query in ,{}, seconds, get ,{}, result points with ,{}, workers (mean rate ,{}, points/s)",
				getQueryName(config),
				config.CLIENT_NUMBER * config.LOOP, totalTime / 1000.0f, totalResultPoint, config.CLIENT_NUMBER,
				(1000.0f * totalResultPoint) / ((float) totalTime));

		long totalErrorPoint = getSumOfList(totalQueryErrorNums);
		LOGGER.info("total error num is {}", totalErrorPoint);

		/*
		LOGGER_RESULT.error("{}: execute,{},query in,{},seconds, get,{},result points with,{},workers, mean rate,{},query/s,{},points/s; Total error point number is,{}",
				getQueryName(config),
				config.CLIENT_NUMBER * config.LOOP,
				totalTime / 1000.0f,
				totalResultPoint,
				config.CLIENT_NUMBER,
				1000.0f * config.CLIENT_NUMBER * config.LOOP / totalTime,
				(1000.0f * totalResultPoint) / ((float) totalTime),
				totalErrorPoint);
		*/

		HashMap<String,String> lastPeriodResults = getLastPeriodResults(config);
		File file = new File(config.LAST_RESULT_PATH + "/lastPeriodResult.txt");
		float lastRate = 1;
		if (file.exists()) {
			LOGGER_RESULT.error("Last period {}: execute {} query in {} seconds, get {} result points with {} workers, mean rate {} query/s ( {} points/s ), total error point num is {} .",
					getQueryName(config),
					lastPeriodResults.get(getQueryName(config).substring(0, 3) + "QueryNum"),
					lastPeriodResults.get(getQueryName(config).substring(0, 3) + "QueryTime"),
					lastPeriodResults.get(getQueryName(config).substring(0, 3) + "QueryResults"),
					lastPeriodResults.get(getQueryName(config).substring(0, 3) + "QueryWorkers"),
					lastPeriodResults.get(getQueryName(config).substring(0, 3) + "QueryRate"),
					lastPeriodResults.get(getQueryName(config).substring(0, 3) + "QueryPointRate"),
					lastPeriodResults.get(getQueryName(config).substring(0, 3) + "QueryErrorNum"));
			lastRate = Float.parseFloat(lastPeriodResults.get(getQueryName(config).substring(0, 3) + "QueryRate"));
		}

		float thisRate = 1000.0f * config.CLIENT_NUMBER * config.LOOP / totalTime;
		LOGGER_RESULT.error("This period {}: execute {} query in {} seconds, get {} result points with {} workers, mean rate {} query/s ( {} points/s ), total error point num is {} . Mean rate change {} %.",
				getQueryName(config),
				config.CLIENT_NUMBER * config.LOOP,
				totalTime / 1000.0f,
				totalResultPoint,
				config.CLIENT_NUMBER,
				thisRate,
				(1000.0f * totalResultPoint) / ((float) totalTime),
				totalErrorPoint,
				((thisRate - lastRate) / lastRate * 100)
		);

		
		mySql.saveResult("queryNumber", ""+config.CLIENT_NUMBER * config.LOOP);
		mySql.saveResult("totalPoint", ""+totalResultPoint);
		mySql.saveResult("totalTime(s)", ""+totalTime / 1000.0f);
		mySql.saveResult("resultPointPerSecond(points/s)", ""+(1000.0f * (totalResultPoint) )/  totalTime);
		mySql.saveResult("totalErrorQuery", ""+totalErrorPoint);

		mySql.closeMysql();
	}

	private static String getQueryName(Config config) throws SQLException {
		switch (config.QUERY_CHOICE){
			case 1:
				return "Exact Point Query";
			case 2:
				return "Fuzzy Point Query";
			case 3:
				return "Aggregation Function Query";
			case 4:
				return "Range Query";
			case 5:
				return "Criteria Query";
			case 6:
				return "Nearest Point Query";
			case 7:
				return "Group By Query";
			default:
				throw new SQLException("unsupported query type " + config.QUERY_CHOICE);
		}
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
