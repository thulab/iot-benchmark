package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;

/**
 * Created by Administrator on 2017/11/16 0016.
 */
public interface IDBFactory {
    IDatebase buildDB() throws SQLException, ClassNotFoundException;
}
