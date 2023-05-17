package cn.edu.tsinghua.iot.benchmark.iotdb110;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class SessionDataSet1 implements ISessionDataSet {
    SessionDataSet sessionDataSet;

    public SessionDataSet1(SessionDataSet sessionDataSet) {
        this.sessionDataSet = sessionDataSet;
    }

    @Override
    public RowRecord next() throws IoTDBConnectionException, StatementExecutionException {
        return sessionDataSet.next();
    }

    @Override
    public boolean hasNext() throws IoTDBConnectionException, StatementExecutionException {
        return sessionDataSet.hasNext();
    }

    @Override
    public void close() throws IoTDBConnectionException, StatementExecutionException {
        sessionDataSet.close();
    }
}
