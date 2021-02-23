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

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.SyntheticWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class IoTDBSessionBase extends IoTDB {

  public IoTDBSessionBase() {
    super();
  }

  public Status insertOneBatchByTablet(Batch batch) {
    return new Status(true);
  }

  public Status insertOneBatchByRecord(Batch batch) {
    return new Status(true);
  }

  public Status insertOneBatchByRecords(Batch batch) {
    return new Status(true);
  }

  protected Tablet genTablet(Batch batch){
    List<MeasurementSchema> schemaList = new ArrayList<>();
    int sensorIndex = 0;
    for (String sensor : batch.getDeviceSchema().getSensors()) {
      String dataType = SyntheticWorkload.getNextDataType(sensorIndex);
      schemaList.add(new MeasurementSchema(sensor, Enum.valueOf(TSDataType.class, dataType),
          Enum.valueOf(TSEncoding.class, getEncodingType(dataType))));
      sensorIndex++;
    }
    String deviceId =
        Constants.ROOT_SERIES_NAME + "." + batch.getDeviceSchema().getGroup() + "." + batch
            .getDeviceSchema().getDevice();
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
        switch (SyntheticWorkload.getNextDataType(sensorIndex)) {
          case "BOOLEAN":
            boolean[] sensorsBool = (boolean[]) values[recordValueIndex];
            sensorsBool[recordIndex] = (boolean) record.getRecordDataValue().get(
                recordValueIndex);
            break;
          case "INT32":
            int[] sensorsInt = (int[]) values[recordValueIndex];
            sensorsInt[recordIndex] = (int) record.getRecordDataValue().get(
                recordValueIndex);
            break;
          case "INT64":
            long[] sensorsLong = (long[]) values[recordValueIndex];
            sensorsLong[recordIndex] = (long) record.getRecordDataValue().get(
                recordValueIndex);
            break;
          case "FLOAT":
            float[] sensorsFloat = (float[]) values[recordValueIndex];
            sensorsFloat[recordIndex] = (float) record.getRecordDataValue().get(
                recordValueIndex);
            break;
          case "DOUBLE":
            double[] sensorsDouble = (double[]) values[recordValueIndex];
            sensorsDouble[recordIndex] = (double) record.getRecordDataValue().get(
                recordValueIndex);
            break;
          case "TEXT":
            Binary[] sensorsText = (Binary[]) values[recordValueIndex];
            sensorsText[recordIndex] = Binary
                .valueOf((String) record.getRecordDataValue().get(recordValueIndex));
            break;
        }
        sensorIndex++;
      }
    }
    return tablet;
  }

  public List<TSDataType> constructDataTypes(int recordValueSize) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int sensorIndex = 0; sensorIndex < recordValueSize;
        sensorIndex++) {
      switch (SyntheticWorkload.getNextDataType(sensorIndex)) {
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

  @Override
  public Status insertOneBatch(Batch batch) {
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
