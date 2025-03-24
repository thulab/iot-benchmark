package cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy.SessionPool;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TableSessionPoolWrapper extends AbstractSessionPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSessionPoolWrapper.class);
  private final ITableSessionPool tableSessionPool;
  private boolean isFirstExecution = true;

  public TableSessionPoolWrapper(DBConfig dbConfig, Integer maxSize) {
    super(dbConfig);
    tableSessionPool = builderTableSessionPool(getHostUrls(), maxSize);
  }

  private ITableSessionPool builderTableSessionPool(List<String> hostUrls, Integer maxSize) {
    return new TableSessionPoolBuilder()
        .nodeUrls(hostUrls)
        .user(dbConfig.getUSERNAME())
        .password(dbConfig.getPASSWORD())
        .enableCompression(config.isENABLE_THRIFT_COMPRESSION())
        .enableRedirection(true)
        .enableAutoFetch(false)
        .maxSize(maxSize)
        .build();
  }

  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    tableSessionPool.getSession().executeNonQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    return tableSessionPool.getSession().executeQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws TsdbException, IoTDBConnectionException, StatementExecutionException {
    return tableSessionPool.getSession().executeQueryStatement(sql, timeoutInMs);
  }

  @Override
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void insertTablet(Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException {
    ITableSession tableSession = tableSessionPool.getSession();
    if (isFirstExecution) {
      StringBuilder sql = new StringBuilder();
      sql.append("use ").append(dbConfig.getDB_NAME()).append("_").append(deviceSchema.getGroup());
      tableSession.executeNonQueryStatement(sql.toString());
      isFirstExecution = false;
    }
    tableSession.insert(tablet);
  }

  @Override
  public void open() {
    // TODO sessionPool是否需要 open
    // TableSession combines the build and open operations of the session
  }

  @Override
  public void close() throws TsdbException {
    try {
      tableSessionPool.close();
    } catch (RuntimeException e) {
      LOGGER.error("Failed to close TableSession");
      throw new TsdbException(e);
    }
  }

  @Override
  public void createSchemaTemplate(Template template)
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void setSchemaTemplate(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void createAlignedTimeseries(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }

  @Override
  public void createMultiTimeseries(
      List<String> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    throw new UnsupportedOperationException("TableSession does not implement this function");
  }
}
