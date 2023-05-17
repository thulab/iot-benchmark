package cn.edu.tsinghua.iot.benchmark.iotdb110;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public interface ISessionDataSet {
    RowRecord next() throws IoTDBConnectionException, StatementExecutionException;
    boolean hasNext() throws IoTDBConnectionException, StatementExecutionException;
    void close() throws IoTDBConnectionException, StatementExecutionException;

    static ISessionDataSet getISessionDataSet(SessionDataSet sessionDataSet) {
        return new SessionDataSet1(sessionDataSet);
    }

    static ISessionDataSet getISessionDataSet(SessionDataSetWrapper sessionDataSetWrapper) {
        return new SessionDataSet2(sessionDataSetWrapper);
    }
}

