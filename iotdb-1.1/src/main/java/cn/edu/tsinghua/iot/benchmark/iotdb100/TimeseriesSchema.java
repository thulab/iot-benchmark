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

package cn.edu.tsinghua.iot.benchmark.iotdb100;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;

import java.util.List;

public class TimeseriesSchema {
  private DeviceSchema deviceSchema;
  private List<String> paths;
  private List<TSDataType> tsDataTypes;
  private List<TSEncoding> tsEncodings;
  private List<CompressionType> compressionTypes;
  private String deviceId;

  public TimeseriesSchema(
      DeviceSchema deviceSchema,
      List<String> paths,
      List<TSDataType> tsDataTypes,
      List<TSEncoding> tsEncodings,
      List<CompressionType> compressionTypes) {
    this.deviceSchema = deviceSchema;
    this.paths = paths;
    this.tsDataTypes = tsDataTypes;
    this.tsEncodings = tsEncodings;
    this.compressionTypes = compressionTypes;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public DeviceSchema getDeviceSchema() {
    return deviceSchema;
  }

  public List<String> getPaths() {
    return paths;
  }

  public List<TSDataType> getTsDataTypes() {
    return tsDataTypes;
  }

  public List<TSEncoding> getTsEncodings() {
    return tsEncodings;
  }

  public List<CompressionType> getCompressionTypes() {
    return compressionTypes;
  }

  public String getDeviceId() {
    return deviceId;
  }
}
