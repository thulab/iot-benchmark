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

package cn.edu.tsinghua.iotdb.benchmark.iotdb009;

import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBUtil;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class IoTDBSession extends IoTDB {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSession.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private final Session session;

  public IoTDBSession() {
    super();
    session =
        new Session(
            config.getHOST().get(0),
            config.getPORT().get(0),
            config.getUSERNAME(),
            config.getPASSWORD());
    try {
      session.open();
    } catch (IoTDBSessionException e) {
      LOGGER.error("Failed to add session", e);
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    List<MeasurementSchema> schemaList = new ArrayList<>();
    int sensorIndex = 0;
    for (String sensor : batch.getDeviceSchema().getSensors()) {
      String dataType = DBUtil.getDataType(sensorIndex);
      schemaList.add(
          new MeasurementSchema(
              sensor,
              Enum.valueOf(TSDataType.class, dataType),
              Enum.valueOf(TSEncoding.class, getEncodingType(dataType))));
      sensorIndex++;
    }
    String deviceId =
        ROOT_SERIES_NAME
            + "."
            + batch.getDeviceSchema().getGroup()
            + "."
            + batch.getDeviceSchema().getDevice();
    RowBatch tablet = new RowBatch(deviceId, schemaList, batch.getRecords().size());
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (int recordIndex = 0; recordIndex < batch.getRecords().size(); recordIndex++) {
      tablet.batchSize++;
      Record record = batch.getRecords().get(recordIndex);
      sensorIndex = 0;
      long currentTime = record.getTimestamp();
      timestamps[recordIndex] = currentTime;
      for (int recordValueIndex = 0;
          recordValueIndex < record.getRecordDataValue().size();
          recordValueIndex++) {
        switch (DBUtil.getDataType(sensorIndex)) {
          case "BOOLEAN":
            boolean[] sensorsBool = (boolean[]) values[recordValueIndex];
            sensorsBool[recordIndex] =
                (boolean) (record.getRecordDataValue().get(recordValueIndex));
            break;
          case "INT32":
            int[] sensorsInt = (int[]) values[recordValueIndex];
            sensorsInt[recordIndex] = (int) (record.getRecordDataValue().get(recordValueIndex));
            break;
          case "INT64":
            long[] sensorsLong = (long[]) values[recordValueIndex];
            sensorsLong[recordIndex] = (long) (record.getRecordDataValue().get(recordValueIndex));
            break;
          case "FLOAT":
            float[] sensorsFloat = (float[]) values[recordValueIndex];
            sensorsFloat[recordIndex] = (float) (record.getRecordDataValue().get(recordValueIndex));
            break;
          case "DOUBLE":
            double[] sensorsDouble = (double[]) values[recordValueIndex];
            sensorsDouble[recordIndex] =
                (double) (record.getRecordDataValue().get(recordValueIndex));
            break;
          case "TEXT":
            Binary[] sensorsText = (Binary[]) values[recordValueIndex];
            sensorsText[recordIndex] =
                Binary.valueOf((String) (record.getRecordDataValue().get(recordValueIndex)));
            break;
        }
        sensorIndex++;
      }
    }
    try {
      session.insertBatch(tablet);
      tablet.reset();
      return new Status(true);
    } catch (IoTDBSessionException e) {
      System.out.println("failed!");
      throw new DBConnectException(e.getMessage());
    }
  }
}
