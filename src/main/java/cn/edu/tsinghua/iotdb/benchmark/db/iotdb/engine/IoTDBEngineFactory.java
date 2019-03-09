package cn.edu.tsinghua.iotdb.benchmark.db.iotdb.engine;


import cn.edu.tsinghua.iotdb.benchmark.db.IDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import java.sql.SQLException;

public class IoTDBEngineFactory implements IDBFactory{

  @Override
  public IDatebase buildDB(long labID) throws SQLException, ClassNotFoundException {
    return new IoTDBEngine(labID);
  }
}
