package cn.edu.tsinghua.iot.benchmark.iotdb130;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.read.common.RowRecord;

public interface ISessionDataSet {
  RowRecord next() throws IoTDBConnectionException, StatementExecutionException;

  boolean hasNext() throws IoTDBConnectionException, StatementExecutionException;

  void close() throws IoTDBConnectionException, StatementExecutionException;

  SessionDataSet.DataIterator iterator();
}
