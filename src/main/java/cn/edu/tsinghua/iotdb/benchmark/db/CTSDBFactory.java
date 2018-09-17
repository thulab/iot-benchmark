package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;

public class CTSDBFactory implements IDBFactory {

    @Override
    public IDatebase buildDB(long labID) throws SQLException, ClassNotFoundException {
        return new CTSDB(labID);
    }
}
