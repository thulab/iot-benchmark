package cn.edu.tsinghua.iot.benchmark.iotdb110;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public interface ISessionDataSet {
  RowRecord next() throws IoTDBConnectionException, StatementExecutionException;

  boolean hasNext() throws IoTDBConnectionException, StatementExecutionException;

  void close() throws IoTDBConnectionException, StatementExecutionException;
}
