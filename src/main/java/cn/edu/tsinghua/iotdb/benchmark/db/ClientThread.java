package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;

public class ClientThread implements Runnable{
	private IDatebase database;
	private int index;
	private Config config;
	
	public ClientThread(IDatebase datebase, int index) {
		// TODO Auto-generated constructor stub
		this.database = datebase;
		this.index = index;
		this.config = Config.newInstance();
	}
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		int i = 0;
		int tmp = 0;
		while(tmp < 2){
			try {
				database.init();
				System.out.println("start");
				database.insertOneBatch(config.DEVICE_CODES.get(index), i);
				System.out.println(Thread.currentThread().getName()+" Batch "+index);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			i++;
			tmp++;
		}
		try {
			database.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
