package cn.edu.tsinghua.iotdb.benchmark.db.opentsdb;

import cn.edu.tsinghua.iotdb.benchmark.db.IDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;

import java.sql.SQLException;

public class OpenTSDBFactory implements IDBFactory {

	@Override
	public IDatebase buildDB(long labID) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		return new OpenTSDB(labID);
	}

}
