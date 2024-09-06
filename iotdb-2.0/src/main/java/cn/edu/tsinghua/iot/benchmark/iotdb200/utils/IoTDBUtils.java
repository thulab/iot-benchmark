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

package cn.edu.tsinghua.iot.benchmark.iotdb200.utils;

import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IoTDBUtils {

  public IoTDBUtils() {}

  public static List<TSDataType> constructDataTypes(List<Sensor> sensors, int recordValueSize) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int sensorIndex = 0; sensorIndex < recordValueSize; sensorIndex++) {
      switch (sensors.get(sensorIndex).getSensorType()) {
        case BOOLEAN:
          dataTypes.add(TSDataType.BOOLEAN);
          break;
        case INT32:
          dataTypes.add(TSDataType.INT32);
          break;
        case INT64:
          dataTypes.add(TSDataType.INT64);
          break;
        case FLOAT:
          dataTypes.add(TSDataType.FLOAT);
          break;
        case DOUBLE:
          dataTypes.add(TSDataType.DOUBLE);
          break;
        case TEXT:
          dataTypes.add(TSDataType.TEXT);
          break;
        case STRING:
          dataTypes.add(TSDataType.STRING);
          break;
        case BLOB:
          dataTypes.add(TSDataType.BLOB);
          break;
        case TIMESTAMP:
          dataTypes.add(TSDataType.TIMESTAMP);
          break;
        case DATE:
          dataTypes.add(TSDataType.DATE);
          break;
      }
    }
    return dataTypes;
  }

  /**
   * convert deviceSchema to the format
   *
   * @return format, e.g. root.group_1.d_1
   */
  public static String getDevicePath(DeviceSchema deviceSchema, String rootSeriesName) {
    StringBuilder name = new StringBuilder(rootSeriesName);
    name.append(".").append(deviceSchema.getGroup());
    for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
      name.append(".").append(pair.getValue());
    }
    name.append(".").append(deviceSchema.getDevice());
    return name.toString();
  }
}
