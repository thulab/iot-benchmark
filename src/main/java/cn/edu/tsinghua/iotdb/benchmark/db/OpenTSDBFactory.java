package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;

public class OpenTSDBFactory implements IDBFactory {

	@Override
	public IDatebase buildDB(long labID) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		return new OpenTSDB(labID);
	}

}
