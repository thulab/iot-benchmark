package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
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
	private static ThreadLocal<Long> errorCount = new ThreadLocal<Long>(){
		protected Long initialValue(){
			return  (long) 0;
		}
	};
	private CountDownLatch downLatch;
	private ArrayList<Long> totalTimes;
	private ArrayList<Long> totalInsertErrorNums;

	public ClientThread(IDatebase datebase, int index, CountDownLatch downLatch,
			ArrayList<Long> totalTimes, ArrayList<Long> totalInsertErrorNums) {
		this.database = datebase;
		this.index = index;
		this.config = ConfigDescriptor.getInstance().getConfig();
		this.downLatch = downLatch;
		this.totalTimes = totalTimes;
		this.totalInsertErrorNums = totalInsertErrorNums;
	}

	public ClientThread(IDatebase datebase, int index , Storage storage, CountDownLatch downLatch,
			ArrayList<Long> totalTimes, ArrayList<Long> totalInsertErrorNums) {
		this.database = datebase;
		this.index = index;
		this.config = ConfigDescriptor.getInstance().getConfig();
		this.storage = storage;
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
					database.insertOneBatch(cons , i, totalTime, errorCount);
				} catch (SQLException e) {
					LOOGER.error("{} Fail to insert one batch into database becasue {}",Thread.currentThread().getName(), e.getMessage());
				}
				i++;
			}
		}
		else{
			int clientDevicesNum = config.DEVICE_NUMBER/config.CLIENT_NUMBER;
			LinkedList<String> deviceCodes = new LinkedList<>();

			//overflow mode 2 related variables initial
			Random random = new Random(config.QUERY_SEED);
			ArrayList<Integer> before = new ArrayList<>();
            int maxIndex = (int) (config.CACHE_NUM * config.LOOP * config.OVERFLOW_RATIO);
            int currMaxIndexOfDist = config.START_TIMESTAMP_INDEX;
			for(int beforeIndex = 0;beforeIndex < maxIndex; beforeIndex++){
			    before.add(beforeIndex);
            }

			for (int m = 0; m < clientDevicesNum; m++) {
				deviceCodes.add(config.DEVICE_CODES.get(index * clientDevicesNum + m));
			}
			while(i < config.LOOP){
				if(config.BENCHMARK_WORK_MODE.equals(Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH)){
					try {
						database.insertGenDataOneBatch(config.STORAGE_GROUP_NAME + "." + config.TIMESERIES_NAME, i, totalTime, errorCount);
					} catch (SQLException e) {
						LOOGER.error("{} Fail to insert one batch into database becasue {}", Thread.currentThread().getName(), e.getMessage());
					}
				}else if(config.MUL_DEV_BATCH){
					try {
						database.insertOneBatchMulDevice(deviceCodes, i, totalTime, errorCount);
					} catch (SQLException e) {
						LOOGER.error("{} Fail to insert one batch into database becasue {}", Thread.currentThread().getName(), e.getMessage());
					}
				}else if(config.OVERFLOW_MODE==0){
					try {
						for (int m = 0; m < clientDevicesNum; m++) {
							database.insertOneBatch(config.DEVICE_CODES.get(index * clientDevicesNum + m), i, totalTime, errorCount);
						}
					} catch (SQLException e) {
						LOOGER.error("{} Fail to insert one batch into database becasue {}", Thread.currentThread().getName(), e.getMessage());
					}
				}else if(config.IS_OVERFLOW && config.OVERFLOW_MODE==1){
                    try {
                        for (int m = 0; m < clientDevicesNum; m++) {
                            maxIndex = database.insertOverflowOneBatch(config.DEVICE_CODES.get(index * clientDevicesNum + m),
                                    i,
                                    totalTime,
                                    errorCount,
                                    before,
                                    maxIndex,
                                    random);
                        }
                    } catch (SQLException e) {
                        LOOGER.error("{} Fail to insert one batch into database becasue {}", Thread.currentThread().getName(), e.getMessage());
                    }
				}else if(config.IS_OVERFLOW && config.OVERFLOW_MODE==2){
					try {
						for (int m = 0; m < clientDevicesNum; m++) {
							currMaxIndexOfDist = database.insertOverflowOneBatchDist(config.DEVICE_CODES.get(index * clientDevicesNum + m),
									i,
									totalTime,
									errorCount,
									currMaxIndexOfDist,
									random);
						}
					} catch (SQLException e) {
						LOOGER.error("{} Fail to insert one batch into database becasue {}", Thread.currentThread().getName(), e.getMessage());
					}
				}else {
                    System.out.println("unsupported overflow mode:" + config.OVERFLOW_MODE);
                    break;
                }
				i++;
			}
		}


		try {
			database.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.totalTimes.add(totalTime.get());
		this.totalInsertErrorNums.add(errorCount.get());
		this.downLatch.countDown();
	}

}
