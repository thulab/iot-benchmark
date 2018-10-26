package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import org.slf4j.Logger;

public class TSDB {

    protected static void queryErrorProcess(int index, ThreadLocal<Long> errorCount, String sql, long startTimeStamp, long endTimeStamp, Exception e, Logger logger, MySqlLog mySql) {
        errorCount.set(errorCount.get() + 1);
        logger.error("{} execute query failed! Error：{}", Thread.currentThread().getName(), e.getMessage());
        logger.error("执行失败的查询语句：{}", sql);
        mySql.saveQueryProcess(index, 0, (endTimeStamp - startTimeStamp) / 1000000000.0f, "query fail!" + sql);
        e.printStackTrace();
    }
}
