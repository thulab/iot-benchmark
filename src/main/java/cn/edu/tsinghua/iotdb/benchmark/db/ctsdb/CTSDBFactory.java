package cn.edu.tsinghua.iotdb.benchmark.db.ctsdb;

import cn.edu.tsinghua.iotdb.benchmark.db.IDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;

import java.sql.SQLException;

public class CTSDBFactory implements IDBFactory {

    @Override
    public IDatebase buildDB(long labID) throws SQLException, ClassNotFoundException {
        return new CTSDB(labID);
    }
}
