/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBClusterSession extends IoTDB {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBClusterSession.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private SessionPool[] sessions;
  private int currSession;
  private static final int MAX_SESSION_CONNECTION_PER_CLIENT = 3;
  private ExecutorService service;
  private Future<?> future;


  public IoTDBClusterSession() {
    super();
    try {
      createSessions();
      this.service = Executors.newSingleThreadExecutor();
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Failed to add session", e);
    }
  }

  private void createSessions() throws IoTDBConnectionException {
    sessions = new SessionPool[config.CLUSTER_HOSTS.size()];
    for (int i = 0; i < sessions.length; i++) {
      String[] split = config.CLUSTER_HOSTS.get(i).split(":");

      if (config.ENABLE_THRIFT_COMPRESSION) {
        sessions[i] = new SessionPool(split[0], Integer.parseInt(split[1]), Constants.USER,
            Constants.PASSWD,
            MAX_SESSION_CONNECTION_PER_CLIENT, true);
      } else {
        sessions[i] = new SessionPool(split[0], Integer.parseInt(split[1]), Constants.USER,
            Constants.PASSWD,
            MAX_SESSION_CONNECTION_PER_CLIENT, false);
      }
    }
  }


  public List<TSDataType> constructDataTypes(int recordValueSize){
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int sensorIndex = 0; sensorIndex < recordValueSize;
        sensorIndex++) {
      switch (getNextDataType(sensorIndex)) {
        case "BOOLEAN":
          dataTypes.add(TSDataType.BOOLEAN);
          break;
        case "INT32":
          dataTypes.add(TSDataType.INT32);
          break;
        case "INT64":
          dataTypes.add(TSDataType.INT64);
          break;
        case "FLOAT":
          dataTypes.add(TSDataType.FLOAT);
          break;
        case "DOUBLE":
          dataTypes.add(TSDataType.DOUBLE);
          break;
        case "TEXT":
          dataTypes.add(TSDataType.TEXT);
          break;
      }
    }
    return dataTypes;
  }

  public Status insertOneBatchByRecord(Batch batch) {
    String deviceId = Constants.ROOT_SERIES_NAME + "." + batch.getDeviceSchema().getGroup() + "." +
        batch.getDeviceSchema().getDevice();
    int failRecord = 0;
    for (Record record : batch.getRecords()) {
      long timestamp = record.getTimestamp();
      List<TSDataType> dataTypes = constructDataTypes(record.getRecordDataValue().size());
      try {
        sessions[currSession].insertRecord(deviceId,timestamp,batch.getDeviceSchema().getSensors(), dataTypes, record.getRecordDataValue());
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        LOGGER.error("insert record failed", e);
        failRecord++;
      }
    }
    currSession = (currSession + 1) % sessions.length;
    Exception e = new Exception("failRecord number is " + failRecord);
    if (failRecord == 0)
      return new Status(true);
    else
      return new Status(false, 0, e, e.toString());
  }

  public Status insertOneBatchByRecords(Batch batch) {
    List<String> deviceIds = new ArrayList<>();
    String deviceId = Constants.ROOT_SERIES_NAME + "." + batch.getDeviceSchema().getGroup() + "." +
        batch.getDeviceSchema().getDevice();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    for (Record record : batch.getRecords()) {
      deviceIds.add(deviceId);
      times.add(record.getTimestamp());
      measurementsList.add(batch.getDeviceSchema().getSensors());
      valuesList.add(record.getRecordDataValue());
      typesList.add(constructDataTypes(record.getRecordDataValue().size()));
    }

    future = service.submit(() -> {
      try {
        sessions[currSession].insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        LOGGER.error("insert records failed", e);
      }
    });

    try {
      future.get(config.WRITE_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      future.cancel(true);
      return new Status(false, 0, e, e.toString());
    }

    currSession = (currSession + 1) % sessions.length;
    return new Status(true);
  }

  public Status insertOneBatchByTablet(Batch batch) {
    List<MeasurementSchema> schemaList = new ArrayList<>();
    int sensorIndex = 0;
    for (String sensor : batch.getDeviceSchema().getSensors()) {
      String dataType = getNextDataType(sensorIndex);
      schemaList.add(new MeasurementSchema(sensor, Enum.valueOf(TSDataType.class, dataType),
          Enum.valueOf(TSEncoding.class, getEncodingType(dataType))));
      sensorIndex++;
    }
    String deviceId =
        Constants.ROOT_SERIES_NAME + "." + batch.getDeviceSchema().getGroup() + "." + batch
            .getDeviceSchema()
            .getDevice();
    Tablet tablet = new Tablet(deviceId, schemaList, batch.getRecords().size());
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (int recordIndex = 0; recordIndex < batch.getRecords().size(); recordIndex++) {
      tablet.rowSize++;
      Record record = batch.getRecords().get(recordIndex);
      sensorIndex = 0;
      long currentTime = record.getTimestamp();
      timestamps[recordIndex] = currentTime;
      for (int recordValueIndex = 0; recordValueIndex < record.getRecordDataValue().size();
          recordValueIndex++) {
        switch (getNextDataType(sensorIndex)) {
          case "BOOLEAN":
            boolean[] sensorsBool = (boolean[]) values[recordValueIndex];
            sensorsBool[recordIndex] = (boolean) record.getRecordDataValue().get(recordValueIndex);
            break;
          case "INT32":
            int[] sensorsInt = (int[]) values[recordValueIndex];
            sensorsInt[recordIndex] = (int) record.getRecordDataValue().get(recordValueIndex);
            break;
          case "INT64":
            long[] sensorsLong = (long[]) values[recordValueIndex];
            sensorsLong[recordIndex] = (long) record.getRecordDataValue().get(recordValueIndex);
            break;
          case "FLOAT":
            float[] sensorsFloat = (float[]) values[recordValueIndex];
            sensorsFloat[recordIndex] = (float) record.getRecordDataValue().get(recordValueIndex);
            break;
          case "DOUBLE":
            double[] sensorsDouble = (double[]) values[recordValueIndex];
            sensorsDouble[recordIndex] = (double) record.getRecordDataValue().get(recordValueIndex);
            break;
          case "TEXT":
            Binary[] sensorsText = (Binary[]) values[recordValueIndex];
            sensorsText[recordIndex] =
                Binary.valueOf((String) record.getRecordDataValue().get(recordValueIndex));
            break;
        }
        sensorIndex++;
      }
    }

    future = service.submit(() -> {
      try {
        sessions[currSession].insertTablet(tablet);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        LOGGER.error("insert tablet failed", e);
      }
    });

    try {
      future.get(config.WRITE_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      future.cancel(true);
      return new Status(false, 0, e, e.toString());
    }

    currSession = (currSession + 1) % sessions.length;
    tablet.reset();
    return new Status(true);
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    if (ioTDBConnection != null) {
      try {
        // in this implementation the connection is only for schema creation, so it is unneeded
        // when the ingestion begins
        ioTDBConnection.close();
      } catch (TsdbException e) {
        LOGGER.error("Cannot close connection for schema creation");
      }
      ioTDBConnection = null;
    }

    switch (config.INSERT_MODE) {
      case "sessionByTablet":
        return insertOneBatchByTablet(batch);
      case "sessionByRecord":
        return insertOneBatchByRecord(batch);
      case "sessionByRecords":
        return insertOneBatchByRecords(batch);
      default:
        throw new IllegalStateException("Unexpected INSERT_MODE value: " + config.INSERT_MODE);
    }
  }
}
