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

public class ClientThread implements Runnable{
	private IDatebase database;
	private int index;
	private Config config;
	private static final Logger LOOGER = LoggerFactory.getLogger(ClientThread.class);
	private Storage storage;
	private static ThreadLocal<Long> totalTime = new ThreadLocal<Long>(){
		protected Long initialValue(){
			return  (long) 0;
		}
	};
	private CountDownLatch downLatch;
	private ArrayList<Long> totalTimes;


	public ClientThread(IDatebase datebase, int index, CountDownLatch downLatch, ArrayList<Long> totalTimes) {
		this.database = datebase;
		this.index = index;
		this.config = ConfigDescriptor.getInstance().getConfig();
		this.downLatch = downLatch;
		this.totalTimes = totalTimes;
	}

	public ClientThread(IDatebase datebase, int index , Storage storage, CountDownLatch downLatch, ArrayList<Long> totalTimes) {
		this.database = datebase;
		this.index = index;
		this.config = ConfigDescriptor.getInstance().getConfig();
		this.storage = storage;
		this.downLatch = downLatch;
		this.totalTimes = totalTimes;
	}


	@Override
	public void run() {
		int i = 0;
		try {
			database.init();
		} catch (SQLException e1) {
			LOOGER.error("{} Fail to init database becasue {}",Thread.currentThread().getName(), e1.getMessage());
			return;
		}

		if(config.READ_FROM_FILE){
			while(true){
				try {
					LinkedList<String> cons = storage.consume(config.BATCH_OP_NUM);
					//线程结束
					if(cons.size() == 0) {
						break;
					}
					database.insertOneBatch(cons , i, totalTime);
				} catch (SQLException e) {
					LOOGER.error("{} Fail to insert one batch into database becasue {}",Thread.currentThread().getName(), e.getMessage());
				}
				i++;
			}
		}
		else{
			int clientDevicesNum = config.DEVICE_NUMBER/config.CLIENT_NUMBER;
			while(i < config.LOOP){
				try {
					for(int m = 0;m < clientDevicesNum;m++){
						database.insertOneBatch(config.DEVICE_CODES.get(index*clientDevicesNum+m), i, totalTime);
					}
				} catch (SQLException e) {
					LOOGER.error("{} Fail to insert one batch into database becasue {}",Thread.currentThread().getName(), e.getMessage());
				}
				i++;
			}
		}


		try {
			database.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.totalTimes.add(totalTime.get());
		this.downLatch.countDown();
	}

}
