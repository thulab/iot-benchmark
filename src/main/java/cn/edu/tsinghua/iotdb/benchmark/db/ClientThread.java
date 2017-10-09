package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;

public class ClientThread implements Runnable{
	private IDatebase database;
	private int index;
	private Config config;
	private static final Logger LOOGER = LoggerFactory.getLogger(ClientThread.class);
	
	public ClientThread(IDatebase datebase, int index) {
		this.database = datebase;
		this.index = index;
		this.config = ConfigDescriptor.getInstance().getConfig();
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
		while(i < config.LOOP){
			try {
				database.insertOneBatch(config.DEVICE_CODES.get(index), i);
			} catch (SQLException e) {
				LOOGER.error("{} Fail to insert one batch into database becasue {}",Thread.currentThread().getName(), e.getMessage());
			}
			i++;
		}
		try {
			database.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
