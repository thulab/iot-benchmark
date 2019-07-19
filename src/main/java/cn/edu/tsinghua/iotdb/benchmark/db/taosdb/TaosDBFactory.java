package cn.edu.tsinghua.iotdb.benchmark.db.taosdb;

import cn.edu.tsinghua.iotdb.benchmark.db.IDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import java.sql.SQLException;

public class TaosDBFactory implements IDBFactory {

  @Override
  public IDatebase buildDB(long labID) throws SQLException, ClassNotFoundException {
    return new TaosDB(labID);
  }
}
