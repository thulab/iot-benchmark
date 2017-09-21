package cn.edu.tsinghua.iotdb.benchmark;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.db.ClientThread;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.IoTDB;

public class App {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		CommandCli cli = new CommandCli();
		if(!cli.init(args)){
			return;
		}
		
		Config config = Config.newInstance();
		config.initDeviceCodes();
		config.initSensorCodes();
		config.initSensorFunction();
		
//		IDatebase datebase = new IoTDB();
//		datebase.init();
//		datebase.createSchema();
//		datebase.close();
		
		System.out.println(config.DEVICE_NUMBER);
		ExecutorService executorService = Executors.newFixedThreadPool(config.DEVICE_NUMBER);
		for(int i = 0; i < config.DEVICE_NUMBER;i++){
			executorService.submit(new ClientThread(new IoTDB(), i));
		}
	}
}
