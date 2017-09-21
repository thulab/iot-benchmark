package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;

public class ClientThread implements Runnable{
	private IDatebase database;
	private int index;
	private Config config;
	
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
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while(i < config.LOOP){
			try {
				database.insertOneBatch(config.DEVICE_CODES.get(index), i);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
