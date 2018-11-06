package cn.edu.tsinghua.iotdb.benchmark.db.timescaledb;

import cn.edu.tsinghua.iotdb.benchmark.db.IDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;

import java.sql.SQLException;

public class TimescaleDBFactory implements IDBFactory {
    @Override
    public IDatebase buildDB(long labID) throws SQLException, ClassNotFoundException {
        return new TimescaleDB(labID);
    }
}
