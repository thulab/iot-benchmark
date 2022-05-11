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

package cn.edu.tsinghua.iotdb.benchmark.iotdb012;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;

public class TemplateSchema {
  private List<List<String>> measurementList;
  private List<List<TSDataType>> dataTypeList;
  private List<List<TSEncoding>> encodingList;
  private List<CompressionType> compressionTypes;
  private List<String> schemaNames;

  public TemplateSchema(
      List<List<String>> measurementList,
      List<List<TSDataType>> dataTypeList,
      List<List<TSEncoding>> encodingList,
      List<CompressionType> compressionTypes,
      List<String> schemaNames) {
    this.measurementList = measurementList;
    this.dataTypeList = dataTypeList;
    this.encodingList = encodingList;
    this.compressionTypes = compressionTypes;
    this.schemaNames = schemaNames;
  }

  public List<List<String>> getMeasurementList() {
    return measurementList;
  }

  public List<List<TSDataType>> getDataTypeList() {
    return dataTypeList;
  }

  public List<List<TSEncoding>> getEncodingList() {
    return encodingList;
  }

  public List<CompressionType> getCompressionTypes() {
    return compressionTypes;
  }

  public List<String> getSchemaNames() {
    return schemaNames;
  }
}
