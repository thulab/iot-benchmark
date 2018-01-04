package cn.edu.tsinghua.iotdb.benchmark.concurrency;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

public class App {

	public static void main(String[] args) throws ClassNotFoundException{
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
		Config config = ConfigDescriptor.getInstance().getConfig();
		for(int i = 0; i < config.MAX_CONNECTION_NUM;i++){
			cachedThreadPool.execute(new QueryThread(config.CONCURRENCY_URL));
		}

	}

}
