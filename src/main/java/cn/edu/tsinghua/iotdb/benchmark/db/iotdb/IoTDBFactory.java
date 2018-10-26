package cn.edu.tsinghua.iotdb.benchmark.db.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.db.IDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;

import java.sql.SQLException;

/**
 * Created by Administrator on 2017/11/16 0016.
 */
public class IoTDBFactory implements IDBFactory {
    @Override
    public IDatebase buildDB(long labID) throws SQLException, ClassNotFoundException {
        return new IoTDB(labID);
    }
}
