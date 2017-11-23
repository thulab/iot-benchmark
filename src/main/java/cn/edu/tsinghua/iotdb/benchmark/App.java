package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.*;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.*;

import java.io.File;
import java.sql.SQLException;
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

public class App {
	private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws ClassNotFoundException,
			SQLException {

		CommandCli cli = new CommandCli();
		if (!cli.init(args)) {
			return;
		}
		Config config = ConfigDescriptor.getInstance().getConfig();
		if(config.SERVER_MODE) {
			File dir = new File(config.LOG_STOP_FLAG_PATH);
			if(dir.exists()&&dir.isDirectory()) {
				File file = new File(config.LOG_STOP_FLAG_PATH + "/log_stop_flag");
				int interval = config.INTERVAL;
				//检测所需的时间在目前代码的参数下至少为2秒
				LOGGER.info("----------New Test Begin with interval about {} s----------", interval + 2);
				while (true) {
					ArrayList<Float> ioUsageList = IoUsage.getInstance().get();
					ArrayList<Float> netUsageList = IoUsage.getInstance().get();
					LOGGER.info("CPU使用率,{}", ioUsageList.get(0));
					LOGGER.info("内存使用率,{}", MemUsage.getInstance().get());
					LOGGER.info("磁盘IO使用率,{}", ioUsageList.get(1));
					LOGGER.info("eth0接收和发送速率,{},{},KB/s", netUsageList.get(0), netUsageList.get(1));
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
			}else{
				LOGGER.error("LOG_STOP_FLAG_PATH not exist!");
			}
		} else {
			if(config.IS_QUERY_TEST){
				queryTest(config);
			}
			else{
				insertTest(config);
			}
			
		}// else--SERVER_MODE
	}// main

	
	/**
	 * 数据库插入测试
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private static void insertTest(Config config) throws SQLException,
			ClassNotFoundException {
		IDBFactory idbFactory = null;
		switch (config.DB_SWITCH) {
		case Constants.DB_IOT:
			idbFactory = new IoTDBFactory();
			break;
		case Constants.DB_INFLUX:
			idbFactory = new InfluxDBFactory();
			break;
		default:
			throw new SQLException("unsupported database " + config.DB_SWITCH);
		}
		IDatebase datebase;
		long createSchemaStartTime;
		long createSchemaEndTime;
		float createSchemaTime;
		try {
			datebase = idbFactory.buildDB();
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
		if (config.READ_FROM_FILE) {
			CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
			ArrayList<Long> totalTimes = new ArrayList<>();
			Storage storage = new Storage();
			ExecutorService executorService = Executors
					.newFixedThreadPool(config.CLIENT_NUMBER + 1);
			executorService.submit(new Resolve(config.FILE_PATH, storage));
			for (int i = 0; i < config.CLIENT_NUMBER; i++) {
				executorService
						.submit(new ClientThread(idbFactory.buildDB(), i,
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
					"READ_FROM_FILE = ,{}, TAG_PATH = ,{}, STORE_MODE = ,{}, BATCH_OP_NUM = ,{}",
					config.READ_FROM_FILE, config.TAG_PATH, config.STORE_MODE,
					config.BATCH_OP_NUM);
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
				executorService.submit(new ClientThread(idbFactory.buildDB(),
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
			LOGGER.info(
					"GROUP_NUMBER = ,{}, DEVICE_NUMBER = ,{}, SENSOR_NUMBER = ,{}, CACHE_NUM = ,{}, POINT_STEP = ,{}",
					config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER,
					config.CACHE_NUM, config.POINT_STEP);
			LOGGER.info(
					"loaded ,{}, points in ,{},s with ,{}, workers (mean rate ,{}, points/s)",
					config.SENSOR_NUMBER * config.DEVICE_NUMBER * config.LOOP
							* config.CACHE_NUM, totalTime / 1000.0f,
					config.CLIENT_NUMBER,
					(1000.0f * config.SENSOR_NUMBER * config.DEVICE_NUMBER
							* config.LOOP * config.CACHE_NUM)
							/ ((float) totalTime));

		}// else--
		long totalErrorPoint = getSumOfList(totalInsertErrorNums);
		LOGGER.info("total error num is {}, create schema cost {},s",
				totalErrorPoint, createSchemaTime);

	}

	/** 数据库查询测试 
	 * @throws SQLException 
	 * @throws ClassNotFoundException */
	private static void queryTest(Config config) throws SQLException, ClassNotFoundException {
		IDBFactory idbFactory = null;
		switch (config.DB_SWITCH) {
		case Constants.DB_IOT:
			idbFactory = new IoTDBFactory();
			break;
		case Constants.DB_INFLUX:
			idbFactory = new InfluxDBFactory();
			break;
		default:
			throw new SQLException("unsupported database " + config.DB_SWITCH);
		}
		IDatebase datebase = null;
		try{
			datebase = idbFactory.buildDB();
			datebase.init();
		}catch (SQLException e) {
			LOGGER.error("Fail to connect to database becasue {}", e.getMessage());
			return;
		}
		
		CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
		ArrayList<Long> totalTimes = new ArrayList<>();
		ArrayList<Long> totalPoints = new ArrayList<>();
		ArrayList<Long> totalQueryErrorNums = new ArrayList<>();
		ExecutorService executorService = Executors
				.newFixedThreadPool(config.CLIENT_NUMBER);
		for (int i = 0; i < config.CLIENT_NUMBER; i++) {
			executorService.submit(new QueryClientThread(idbFactory.buildDB(),
					i, downLatch, totalTimes, totalPoints, totalQueryErrorNums));
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
				config.DEVICE_NUMBER * config.LOOP
				, totalTime / 1000.0f,
				totalResultPoint,
				config.CLIENT_NUMBER,
				(1000.0f * totalResultPoint)
				/ ((float) totalTime));
		
		long totalErrorPoint = getSumOfList(totalQueryErrorNums);
		LOGGER.info("total error num is {}",totalErrorPoint);
	}
	
	/** 计算list中所有元素的和 */
	private static long getSumOfList(ArrayList<Long> list) {
		long total = 0;
		for (long c : list) {
			total += c;
		}
		return total;
	}
	
	/***/
	

}
