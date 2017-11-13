package cn.edu.tsinghua.iotdb.benchmark;

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
import cn.edu.tsinghua.iotdb.benchmark.db.ClientThread;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.IoTDB;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Resolve;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Storage;
import sun.misc.Cleaner;

public class App {
	private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		CommandCli cli = new CommandCli();
		if(!cli.init(args)){
			return;
		}
		Config config = ConfigDescriptor.getInstance().getConfig();
		if(config.SERVER_MODE) {
			File file = new File("/home/hadoop/liurui/log_stop_flag");
			int interval = config.INTERVAL;
			//检测所需的时间在目前代码的参数下至少为2秒
			LOGGER.info("----------New Test Begin with interval about {} s----------", interval + 2);
			while (true) {
				ArrayList<Float> list = IoUsage.getInstance().get();
				LOGGER.info("CPU使用率,{}", list.get(0));
				LOGGER.info("内存使用率,{}", MemUsage.getInstance().get());
				LOGGER.info("磁盘IO使用率,{}", list.get(1));
				LOGGER.info("eth0接受和发送总速率,{},KB/s", NetUsage.getInstance().get());
				try {
					Thread.sleep(interval * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (file.exists()) {
					boolean f =file.delete();
					if(!f){
						LOGGER.error("log_stop_flag 文件删除失败");
					}
					break;
				}
			}
		}else {
			IDatebase datebase;
			try {
				datebase = new IoTDB();
				datebase.init();
				datebase.createSchema();
				datebase.close();
			} catch (SQLException e) {
				LOGGER.error("Fail to init database becasue {}", e.getMessage());
				return;
			}

			if (config.READ_FROM_FILE) {

				CountDownLatch downLatch = new CountDownLatch(config.DEVICE_NUMBER);
				Storage storage = new Storage();
				ArrayList<Long> totalTimes = new ArrayList<>();

				ExecutorService executorService = Executors.newFixedThreadPool(config.DEVICE_NUMBER + 1);
				executorService.submit(new Resolve(config.FILE_PATH, storage));
				for (int i = 0; i < config.DEVICE_NUMBER; i++) {
					executorService.submit(new ClientThread(new IoTDB(), i, storage, downLatch, totalTimes));
				}
				executorService.shutdown();
				//wait for all threads complete
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
				LOGGER.info("READ_FROM_FILE = {}, TAG_PATH = {}, STORE_MODE = {}, BATCH_OP_NUM = {}",
						config.READ_FROM_FILE,
						config.TAG_PATH,
						config.STORE_MODE,
						config.BATCH_OP_NUM);
				LOGGER.info("loaded {} items in {}s with {} workers (mean rate {} items/s)",
						totalItem,
						totalTime / 1000.0,
						config.DEVICE_NUMBER,
						1000 * totalItem / (double) totalTime);

			} else {

				ExecutorService executorService = Executors.newFixedThreadPool(config.DEVICE_NUMBER);
				for (int i = 0; i < config.DEVICE_NUMBER; i++) {
					executorService.submit(new ClientThread(new IoTDB(), i));
				}
				executorService.shutdown();
			}
		}
	}//main

}
