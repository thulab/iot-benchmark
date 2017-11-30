package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;

/**
 * Created by Administrator on 2017/11/16 0016.
 */
public class InfluxDBFactory implements IDBFactory {
    @Override
    public IDatebase buildDB(long labID) throws SQLException, ClassNotFoundException {
        return new InfluxDBV2();
    }
}
