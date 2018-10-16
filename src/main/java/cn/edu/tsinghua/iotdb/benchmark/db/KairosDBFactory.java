package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;

public class KairosDBFactory implements IDBFactory{
    @Override
    public IDatebase buildDB(long labID) throws SQLException, ClassNotFoundException {
        return new KairosDB(labID);
    }
}
