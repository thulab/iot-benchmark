package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Storage;

public class QueryClientThread implements Runnable {
	private int clientDevicesNum;
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
	
	private Long queryResultPoints[];
	private Long queryResultTimes[];

	public QueryClientThread(IDatebase datebase, int index,
			CountDownLatch downLatch, ArrayList<Long> totalTimes, ArrayList<Long> totalPoints,
			ArrayList<Long> totalQueryErrorNums) {
		this.database = datebase;
		this.index = index;
		this.config = ConfigDescriptor.getInstance().getConfig();
		this.downLatch = downLatch;
		this.totalTimes = totalTimes;
		this.totalPoints = totalPoints;
		this.totalQueryErrorNums = totalQueryErrorNums;
		
		clientDevicesNum = config.DEVICE_NUMBER / config.CLIENT_NUMBER;
		
		queryResultPoints = new Long[clientDevicesNum];
		queryResultTimes = new Long[clientDevicesNum];
		for(int i = 0; i < clientDevicesNum; i++){
			queryResultPoints[i] = (long)0;
			queryResultTimes[i] = (long)0;
		}
		
	}

	@Override
	public void run() {
		int i = 0;
		try {
			database.init();
		} catch (SQLException e1) {
			LOOGER.error("{} Fail to init database becasue {}", Thread
					.currentThread().getName(), e1.getMessage());
			return;
		}

		long startTimeInterval = config.CACHE_NUM * config.POINT_STEP;
		try {
			startTimeInterval = database.getTotalTimeInterval() / config.LOOP;
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			LOOGER.error("{} Fail to get total time interval becasue {}", Thread
					.currentThread().getName(), e1.getMessage());
			e1.printStackTrace();
			return;
		}
		while (i < config.LOOP) {
			for (int m = 0; m < clientDevicesNum; m++) {
				
				database.executeOneQuery(
						config.DEVICE_CODES.get(index * clientDevicesNum + m),
						i, startTimeInterval * i + Constants.START_TIMESTAMP, this, errorCount);
			}
			i++;
		}

		try {
			database.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.totalTimes.add(getSumOfArr(queryResultTimes));
		this.totalPoints.add(getSumOfArr(queryResultPoints));
		this.totalQueryErrorNums.add(errorCount.get());
		for(int j = 0; j < clientDevicesNum; j++){
			LOOGER.info("Thread_{} , device_{} : The result points is {}, the query time is {}s, so rate is {} points/s.", 
					index, index*clientDevicesNum+j, queryResultPoints[j], queryResultTimes[j]/1000.0f, 1000.0f * queryResultPoints[j]/queryResultTimes[j]);
			
		}
		this.downLatch.countDown();
	}

	
	public void addResultPointAndTime(int dev, long pointNum, long time){
		int index = dev % clientDevicesNum;
		queryResultPoints[index] += pointNum;
		queryResultTimes[index] += time;
	}
	
	public long getResultPoint(int dev){
		return queryResultPoints[dev % clientDevicesNum];
	}
	
	public long getResultTime(int dev){
		return queryResultTimes[dev % clientDevicesNum];
	}
	
	/** 计算arr中所有元素的和 */
	private static long getSumOfArr(Long[] arr) {
		long total = 0;
		for (long c : arr) {
			total += c;
		}
		return total;
	}
	
	
}
