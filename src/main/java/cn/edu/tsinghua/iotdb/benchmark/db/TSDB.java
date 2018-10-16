package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.model.KairosDataModel;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;

public class TSDB {

    static void queryErrorProcess(int index, ThreadLocal<Long> errorCount, String sql, long startTimeStamp, long endTimeStamp, Exception e, Logger logger, MySqlLog mySql) {
        errorCount.set(errorCount.get() + 1);
        logger.error("{} execute query failed! Error：{}", Thread.currentThread().getName(), e.getMessage());
        logger.error("执行失败的查询语句：{}", sql);
        mySql.saveQueryProcess(index, 0, (endTimeStamp - startTimeStamp) / 1000000000.0f, "query fail!" + sql);
        e.printStackTrace();
    }

    public static void measure(int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, long startTime, long endTime, String body, String writeUrl, Logger logger, int size, MySqlLog mySql, Config config) throws SQLException {
        String response;
        try {
            startTime = System.nanoTime();
            response = HttpRequest.sendPost(writeUrl, body);
            endTime = System.nanoTime();
            int errorNum = JSON.parseObject(response).getInteger("failed");
            errorCount.set(errorCount.get() + errorNum);
            logger.debug(response);
            logger.info("{} execute ,{}, batch, it costs ,{},s, totalTime ,{},s, throughput ,{}, point/s",
                    Thread.currentThread().getName(), batchIndex, (endTime - startTime) / 1000000000.0,
                    ((totalTime.get() + (endTime - startTime)) / 1000000000.0),
                    (size / (double) (endTime - startTime)) * 1000000000);
            totalTime.set(totalTime.get() + (endTime - startTime));
            mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0, errorNum,
                    config.REMARK);
        } catch (IOException e) {
            errorCount.set(errorCount.get() + size);
            logger.error("Batch insert failed, the failed num is ,{}, Error：{}", size, e.getMessage());
            mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0, size,
                    "error message: " + e.getMessage());
            throw new SQLException(e.getMessage());
        }
    }
}
