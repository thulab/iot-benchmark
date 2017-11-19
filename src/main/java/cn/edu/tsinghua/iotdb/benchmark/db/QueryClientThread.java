package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Storage;

public class QueryClientThread implements Runnable {
	private IDatebase database;
	private int index;
	private Config config;
	private static final Logger LOOGER = LoggerFactory
			.getLogger(QueryClientThread.class);
	private Storage storage;
	private static ThreadLocal<Long> totalTime = new ThreadLocal<Long>() {
		protected Long initialValue() {
			return (long) 0;
		}
	};
	private static ThreadLocal<Long> errorCount = new ThreadLocal<Long>() {
		protected Long initialValue() {
			return (long) 0;
		}
	};
	private CountDownLatch downLatch;
	private ArrayList<Long> totalTimes;
	private ArrayList<Long> totalInsertErrorNums;

	public QueryClientThread(IDatebase datebase, int index,
			CountDownLatch downLatch, ArrayList<Long> totalTimes,
			ArrayList<Long> totalInsertErrorNums) {
		this.database = datebase;
		this.index = index;
		this.config = ConfigDescriptor.getInstance().getConfig();
		this.downLatch = downLatch;
		this.totalTimes = totalTimes;
		this.totalInsertErrorNums = totalInsertErrorNums;
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

		int clientDevicesNum = config.DEVICE_NUMBER / config.CLIENT_NUMBER;
		while (i < config.LOOP) {
			for (int m = 0; m < clientDevicesNum; m++) {
				database.executeOneQuery(
						config.DEVICE_CODES.get(index * clientDevicesNum + m),
						i, totalTime, errorCount);
			}
			i++;
		}

		try {
			database.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.totalTimes.add(totalTime.get());
		this.totalInsertErrorNums.add(errorCount.get());
		this.downLatch.countDown();
	}

}
