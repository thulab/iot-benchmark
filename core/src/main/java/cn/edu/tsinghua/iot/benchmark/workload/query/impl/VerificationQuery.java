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

package cn.edu.tsinghua.iot.benchmark.workload.query.impl;

import cn.edu.tsinghua.iot.benchmark.entity.Batch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;

import java.util.List;

public class VerificationQuery extends Query {
  private DeviceSchema deviceSchema;
  private List<Record> records;

  public VerificationQuery() {}

  public VerificationQuery(Batch batch) {
    this.deviceSchema = batch.getDeviceSchema();
    this.records = batch.getRecords();
  }

  public DeviceSchema getDeviceSchema() {
    return deviceSchema;
  }

  public List<Record> getRecords() {
    return records;
  }

  @Override
  public StringBuilder getQueryAttrs() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("deviceSchema=").append(deviceSchema);
    stringBuilder.append(" records=").append(records);
    return stringBuilder;
  }
}
