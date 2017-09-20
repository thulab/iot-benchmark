package cn.edu.tsinghua.iotdb.benchmark;

import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.IoTDB;
import cn.edu.tsinghua.tsfile.common.utils.Pair;

public class App {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Config config = Config.newInstance();
		IDatebase datebase = new IoTDB();
		datebase.init();
		datebase.createSchema();
		datebase.close();
		
		ExecutorService executorService = Executors.newFixedThreadPool(config.DEVICE_NUMBER);
		int index = 0;
		while(index < 10){
			for(int i = 0; i < config.DEVICE_NUMBER;i++){
				executorService.submit(new FutureTask<Pair<Integer, Integer>>(new Callable<Pair<Integer, Integer>>() {
					@Override
					public Pair<Integer, Integer> call() throws Exception {
						// TODO Auto-generated method stub
						return null;
					}
				}));
			}
			index++;
		}
		
		
		

		
		
	}
}
