/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.iotdb011;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IoTDBSessionPool implements IDatabase {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  protected final String ROOT_SERIES_NAME;
  protected DBConfig dbConfig;

  private static final String ALREADY_KEYWORD = "already";
  private static SessionPool pool;
  private volatile boolean isInit = false;

  public IoTDBSessionPool(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    ROOT_SERIES_NAME = "root." + dbConfig.getDB_NAME();
  }

  @Override
  public void init() {
    if (isInit) {
      return;
    }

    org.apache.iotdb.jdbc.Config.rpcThriftCompressionEnable = config.isENABLE_THRIFT_COMPRESSION();
    pool =
        new SessionPool(
            dbConfig.getHOST().get(0),
            Integer.parseInt(dbConfig.getPORT().get(0)),
            dbConfig.getUSERNAME(),
            dbConfig.getPASSWORD(),
            config.getIOTDB_SESSION_POOL_SIZE());
    isInit = true;
  }

  @Override
  public void cleanup() {
    try {
      pool.deleteTimeseries("root." + dbConfig.getDB_NAME());
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.warn("Clean up failed!");
    }
  }

  @Override
  public void close() {
    if (pool != null) {
      pool.close();
      pool = null;
    }
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    long start = System.nanoTime();
    long end;
    int count = 0;
    if (config.hasWrite()) {
      try {
        // get all storage groups
        Set<String> groups = new HashSet<>();
        for (DeviceSchema schema : schemaList) {
          groups.add(schema.getGroup());
        }
        // register storage groups
        for (String group : groups) {
          pool.setStorageGroup(ROOT_SERIES_NAME + "." + group);
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        // ignore if already has the time series
        if (!e.getMessage().contains(ALREADY_KEYWORD)) {
          LOGGER.error("Register IoTDB schema failed because ", e);
          throw new TsdbException(e);
        }
      }

      List<String> paths = new ArrayList<>(5000);
      List<TSDataType> dataTypes = new ArrayList<>(5000);
      List<TSEncoding> encodings = new ArrayList<>(5000);
      List<CompressionType> compressors = new ArrayList<>(5000);
      // create time series
      try {
        for (DeviceSchema deviceSchema : schemaList) {
          for (Sensor sensor : deviceSchema.getSensors()) {
            paths.add(
                ROOT_SERIES_NAME
                    + "."
                    + deviceSchema.getGroup()
                    + "."
                    + deviceSchema.getDevice()
                    + "."
                    + sensor);
            SensorType dataSensorType = sensor.getSensorType();
            dataTypes.add(TSDataType.valueOf(dataSensorType.name));
            encodings.add(TSEncoding.valueOf(getEncodingType(dataSensorType)));
            // TODO remove when [IOTDB-1518] is solved(not supported null)
            compressors.add(CompressionType.valueOf("SNAPPY"));
            count++;
            if (count % 5000 == 0) {
              pool.createMultiTimeseries(
                  paths, dataTypes, encodings, compressors, null, null, null, null);
              paths.clear();
              dataTypes.clear();
              encodings.clear();
              compressors.clear();
            }
          }
        }
        if (!paths.isEmpty()) {
          pool.createMultiTimeseries(
              paths, dataTypes, encodings, compressors, null, null, null, null);
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        // ignore if already has the time series
        if (!e.getMessage().contains(ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
          LOGGER.error("Register IoTDB schema failed because ", e);
          throw new TsdbException(e);
        }
      }
    }
    end = System.nanoTime();
    return TimeUtils.convertToSeconds(end - start, "ns");
  }

  private String getEncodingType(SensorType dataSensorType) {
    switch (dataSensorType) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case TEXT:
        return "PLAIN";
      default:
        LOGGER.error("Unsupported data sensorType {}.", dataSensorType);
        return null;
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    List<MeasurementSchema> schemaList = new ArrayList<>();
    int sensorIndex = 0;
    for (Sensor sensor : batch.getDeviceSchema().getSensors()) {
      SensorType dataSensorType = sensor.getSensorType();
      schemaList.add(
          new MeasurementSchema(
              sensor.getName(),
              Enum.valueOf(TSDataType.class, dataSensorType.name),
              Enum.valueOf(TSEncoding.class, getEncodingType(dataSensorType))));
      sensorIndex++;
    }
    String deviceId =
        ROOT_SERIES_NAME
            + "."
            + batch.getDeviceSchema().getGroup()
            + "."
            + batch.getDeviceSchema().getDevice();
    Tablet tablet = new Tablet(deviceId, schemaList, batch.getRecords().size());
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    List<Sensor> sensors = batch.getDeviceSchema().getSensors();
    for (int recordIndex = 0; recordIndex < batch.getRecords().size(); recordIndex++) {
      tablet.rowSize++;
      Record record = batch.getRecords().get(recordIndex);
      sensorIndex = 0;
      long currentTime = record.getTimestamp();
      timestamps[recordIndex] = currentTime;
      for (int recordValueIndex = 0;
          recordValueIndex < record.getRecordDataValue().size();
          recordValueIndex++) {
        switch (sensors.get(sensorIndex).getSensorType()) {
          case BOOLEAN:
            boolean[] sensorsBool = (boolean[]) values[recordValueIndex];
            sensorsBool[recordIndex] = (boolean) record.getRecordDataValue().get(recordValueIndex);
            break;
          case INT32:
            int[] sensorsInt = (int[]) values[recordValueIndex];
            sensorsInt[recordIndex] = (int) record.getRecordDataValue().get(recordValueIndex);
            break;
          case INT64:
            long[] sensorsLong = (long[]) values[recordValueIndex];
            sensorsLong[recordIndex] = (long) record.getRecordDataValue().get(recordValueIndex);
            break;
          case FLOAT:
            float[] sensorsFloat = (float[]) values[recordValueIndex];
            sensorsFloat[recordIndex] = (float) record.getRecordDataValue().get(recordValueIndex);
            break;
          case DOUBLE:
            double[] sensorsDouble = (double[]) values[recordValueIndex];
            sensorsDouble[recordIndex] = (double) record.getRecordDataValue().get(recordValueIndex);
            break;
          case TEXT:
            // TODO FIXME seems the text is not supported.
            Binary[] sensorsText = (Binary[]) values[recordValueIndex];
            sensorsText[recordIndex] =
                Binary.valueOf((String) (record.getRecordDataValue().get(recordValueIndex)));
            break;
        }
        sensorIndex++;
      }
    }
    try {
      pool.insertTablet(tablet);
      tablet.reset();
      return new Status(true);
    } catch (StatementExecutionException e) {
      System.out.println("failed!");
      return new Status(false, 0, e, e.toString());
    } catch (IoTDBConnectionException e) {
      throw new DBConnectException(e.getMessage());
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return executeQueryAndGetStatus(getPreciseQuerySql(preciseQuery));
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    String sql =
        getRangeQuerySql(
            rangeQuery.getDeviceSchema(),
            rangeQuery.getStartTimestamp(),
            rangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    String sql = getValueRangeQuerySql(valueRangeQuery);
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun());
    String sql =
        addWhereTimeClause(
            aggQuerySqlHead, aggRangeQuery.getStartTimestamp(), aggRangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggValueQuery.getDeviceSchema(), aggValueQuery.getAggFun());
    String sql =
        aggQuerySqlHead
            + " WHERE "
            + getValueFilterClause(
                    aggValueQuery.getDeviceSchema(), (int) aggValueQuery.getValueThreshold())
                .substring(4);
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(), aggRangeValueQuery.getAggFun());
    String sql =
        addWhereTimeClause(
            aggQuerySqlHead,
            aggRangeValueQuery.getStartTimestamp(),
            aggRangeValueQuery.getEndTimestamp());
    sql +=
        getValueFilterClause(
            aggRangeValueQuery.getDeviceSchema(), (int) aggRangeValueQuery.getValueThreshold());
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
    String sql =
        addGroupByClause(
            aggQuerySqlHead,
            groupByQuery.getStartTimestamp(),
            groupByQuery.getEndTimestamp(),
            groupByQuery.getGranularity());
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    String aggQuerySqlHead = getLatestPointQuerySql(latestPointQuery.getDeviceSchema());
    return executeQueryAndGetStatus(aggQuerySqlHead);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    String sql =
        getRangeQuerySql(
                rangeQuery.getDeviceSchema(),
                rangeQuery.getStartTimestamp(),
                rangeQuery.getEndTimestamp())
            + " order by time desc";
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    String sql = getValueRangeQuerySql(valueRangeQuery) + " order by time desc";
    return executeQueryAndGetStatus(sql);
  }

  private String getPreciseQuerySql(PreciseQuery preciseQuery) {
    String strTime = preciseQuery.getTimestamp() + "";
    return getSimpleQuerySqlHead(preciseQuery.getDeviceSchema()) + " WHERE time = " + strTime;
  }

  /**
   * generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT s_0, s_3 FROM root.group_0.d_1, root.group_1.d_2
   */
  private String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<Sensor> querySensors = devices.get(0).getSensors();
    builder.append(querySensors.get(0).getName());
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i).getName());
    }
    return addFromClause(devices, builder);
  }

  private String addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    builder.append(" FROM ").append(getDevicePath(devices.get(0)));
    for (int i = 1; i < devices.size(); i++) {
      builder.append(", ").append(getDevicePath(devices.get(i)));
    }
    return builder.toString();
  }

  // convert deviceSchema to the format: root.group_1.d_1
  private String getDevicePath(DeviceSchema deviceSchema) {
    return ROOT_SERIES_NAME + "." + deviceSchema.getGroup() + "." + deviceSchema.getDevice();
  }

  private Status executeQueryAndGetStatus(String sql) {
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }
    int line = 0;
    int queryResultPointNum = 0;
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = pool.executeQueryStatement(sql);
      DataIterator dataIterator = wrapper.iterator();
      while (dataIterator.next()) {
        line++;
      }
      queryResultPointNum = line * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM();
      return new Status(true, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, queryResultPointNum, e, sql);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum, new Exception(t), sql);
    } finally {
      if (wrapper != null) {
        pool.closeResultSet(wrapper);
      }
    }
  }

  private String getRangeQuerySql(List<DeviceSchema> deviceSchemas, long start, long end) {
    return addWhereTimeClause(getSimpleQuerySqlHead(deviceSchemas), start, end);
  }

  private String addWhereTimeClause(String prefix, long start, long end) {
    String startTime = start + "";
    String endTime = end + "";
    return prefix + " WHERE time >= " + startTime + " AND time <= " + endTime;
  }

  private String addGroupByClause(String prefix, long start, long end, long granularity) {
    return prefix + " group by ([" + start + "," + end + ")," + granularity + "ms) ";
  }

  private String getValueRangeQuerySql(ValueRangeQuery valueRangeQuery) {
    String rangeQuerySql =
        getRangeQuerySql(
            valueRangeQuery.getDeviceSchema(),
            valueRangeQuery.getStartTimestamp(),
            valueRangeQuery.getEndTimestamp());
    String valueFilterClause =
        getValueFilterClause(
            valueRangeQuery.getDeviceSchema(), (int) valueRangeQuery.getValueThreshold());
    return rangeQuerySql + valueFilterClause;
  }

  private String getValueFilterClause(List<DeviceSchema> deviceSchemas, int valueThreshold) {
    StringBuilder builder = new StringBuilder();
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        builder
            .append(" AND ")
            .append(getDevicePath(deviceSchema))
            .append(".")
            .append(sensor.getName())
            .append(" > ")
            .append(valueThreshold);
      }
    }
    return builder.toString();
  }

  private String getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<Sensor> querySensors = devices.get(0).getSensors();
    builder.append(aggFun).append("(").append(querySensors.get(0).getName()).append(")");
    for (int i = 1; i < querySensors.size(); i++) {
      builder
          .append(", ")
          .append(aggFun)
          .append("(")
          .append(querySensors.get(i).getName())
          .append(")");
    }
    return addFromClause(devices, builder);
  }

  private String getLatestPointQuerySql(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT last ");
    List<Sensor> querySensors = devices.get(0).getSensors();
    builder.append(querySensors.get(0).getName());
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i).getName());
    }
    return addFromClause(devices, builder);
  }
}
