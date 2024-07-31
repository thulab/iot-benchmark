package cn.edu.tsinghua.iot.benchmark.iotdb130;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;

import java.util.List;

public interface IBenchmarkSession {
  void open() throws IoTDBConnectionException;

  void open(boolean enableRPCCompression) throws IoTDBConnectionException;

  void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedRecord(
      String multiSeriesId,
      long time,
      List<String> multiMeasurementComponents,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedRecords(
      List<String> multiSeriesIds,
      List<Long> times,
      List<List<String>> multiMeasurementComponentsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertTablet(Tablet tablet) throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException;

  ISessionDataSet executeQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException;

  void executeNonQueryStatement(String deleteSeriesSql)
      throws IoTDBConnectionException, StatementExecutionException;

  void close() throws IoTDBConnectionException;
}
