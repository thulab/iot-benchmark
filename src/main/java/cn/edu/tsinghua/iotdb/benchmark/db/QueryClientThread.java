package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Storage;

public class QueryClientThread implements Runnable {
	private IDatebase database;
	private int index;
	private Config config;
	private static final Logger LOOGER = LoggerFactory
			.getLogger(QueryClientThread.class);
	
	private static ThreadLocal<Long> errorCount = new ThreadLocal<Long>() {
		protected Long initialValue() {
			return (long) 0;
		}
	};
	private CountDownLatch downLatch;
	private ArrayList<Long> totalTimes;
	private ArrayList<Long> totalPoints;
	private ArrayList<Long> totalQueryErrorNums;
	private ArrayList<Long> latencies;
	private ArrayList<ArrayList> latenciesOfClients;
	private Long totalTime;
	private Long totalPoint;
	
	private Random deviceRandom = null; 

	public QueryClientThread(IDatebase datebase, int index,
			CountDownLatch downLatch, ArrayList<Long> totalTimes, ArrayList<Long> totalPoints,
			ArrayList<Long> totalQueryErrorNums, ArrayList<ArrayList> latenciesOfClients) {
		this.database = datebase;
		this.index = index;
		this.config = ConfigDescriptor.getInstance().getConfig();
		this.downLatch = downLatch;
		this.totalTimes = totalTimes;
		this.totalPoints = totalPoints;
		this.totalQueryErrorNums = totalQueryErrorNums;
		this.latencies = new ArrayList<>();
		this.latenciesOfClients = latenciesOfClients;
		totalTime = (long)0;
		totalPoint = (long)0;
		
		deviceRandom = new Random(2 * index + config.QUERY_SEED);
	}

	@Override
	public void run() {
		int i = 0;

		long startTimeInterval = config.CACHE_NUM * config.POINT_STEP;
//		try {
//			startTimeInterval = database.getTotalTimeInterval() / config.LOOP;
//		} catch (SQLException e1) {
//			// TODO Auto-generated catch block
//			LOOGER.error("{} Fail to get total time interval becasue {}", Thread
//					.currentThread().getName(), e1.getMessage());
//			e1.printStackTrace();
//			return;
//		}
		List<Integer> clientDevicesIndex = new ArrayList<Integer>();
		List<Integer> queryDevicesIndex = new ArrayList<Integer>();
		
		/**每一个设备只查询clientDevicesNum个设备*/
//		clientDevicesNum = config.DEVICE_NUMBER / config.CLIENT_NUMBER;		
//		for (int m = 0; m < clientDevicesNum; m++){
//			clientDevicesIndex.add(index + m * config.CLIENT_NUMBER);
//		}
		
		for (int m = 0; m < config.DEVICE_NUMBER; m++){
			clientDevicesIndex.add( m );
		}
		while (i < config.LOOP) {
			Collections.shuffle(clientDevicesIndex, deviceRandom);
			for (int m = 0; m < config.QUERY_DEVICE_NUM; m++){
				queryDevicesIndex.add(clientDevicesIndex.get(m));
			}
			database.executeOneQuery(queryDevicesIndex,
					i, startTimeInterval * i + Constants.START_TIMESTAMP, this, errorCount, latencies);
			i++;
			queryDevicesIndex.clear();
		}

		try {
			database.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		this.totalTimes.add(totalTime);
		this.totalPoints.add(totalPoint);
		this.totalQueryErrorNums.add(errorCount.get());
		this.latenciesOfClients.add(latencies);
		this.downLatch.countDown();
	}

	public Long getTotalTime() {
		return totalTime;
	}

	public void setTotalTime(Long totalTime) {
		this.totalTime = totalTime;
	}

	public Long getTotalPoint() {
		return totalPoint;
	}

	public void setTotalPoint(Long totalPoint) {
		this.totalPoint = totalPoint;
	}
	
	public int getQueryClientIndex() {
		return index;
	}
	
}
