package cn.edu.tsinghua.iotdb.benchmark;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.db.ClientThread;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.IoTDB;

public class App {
	private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		CommandCli cli = new CommandCli();
		if(!cli.init(args)){
			return;
		}
		Config config = ConfigDescriptor.getInstance().getConfig();
		IDatebase datebase;
		try {
			datebase = new IoTDB();
			datebase.init();
			datebase.createSchema();
			datebase.close();
		} catch (SQLException e) {
			LOGGER.error("Fail to init database becasue {}",e.getMessage());
			return;
		}
		
		ExecutorService executorService = Executors.newFixedThreadPool(config.DEVICE_NUMBER);
		for(int i = 0; i < config.DEVICE_NUMBER;i++){
			executorService.submit(new ClientThread(new IoTDB(), i));
		}
		executorService.shutdown();
	}
}
