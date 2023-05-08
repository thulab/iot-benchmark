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

package cn.edu.tsinghua.iot.benchmark.entity.Batch;

import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MultiDeviceBatch implements IBatch {

  private final ArrayList<DeviceSchema> deviceSchemas;

  private final ArrayList<List<Record>> recordLists;

  private int index = 0;

  public MultiDeviceBatch(int size) {
    this.deviceSchemas = new ArrayList<>(size);
    this.recordLists = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      deviceSchemas.add(null);
      recordLists.add(new LinkedList<>());
    }
  }

  @Override
  public DeviceSchema getDeviceSchema() {
    return deviceSchemas.get(index);
  }

  @Override
  public List<Record> getRecords() {
    return recordLists.get(index);
  }

  @Override
  public boolean hasNext() {
    return index < deviceSchemas.size() - 1;
  }

  @Override
  public void next() {
    index++;
    if (index >= deviceSchemas.size()) {
      throw new IndexOutOfBoundsException("MultiDeviceBatch index out of bound");
    }
  }

  @Override
  public void setColIndex(int sensorId) {
    // do nothing
  }

  @Override
  public int getColIndex() {
    return -1; // always treated as align
  }

  @Override
  public void setDeviceSchema(DeviceSchema deviceSchema) {
    deviceSchemas.set(index, deviceSchema);
  }

  @Override
  public void add(long currentTimestamp, List<Object> values) {
    recordLists.get(index).add(new Record(currentTimestamp, values));
  }

  @Override
  public long pointNum() {
    long pointNum = 0;
    for (Record record : recordLists.get(index)) {
      pointNum += record.size();
    }
    return pointNum;
  }

  // every MultiDeviceBatch needs to be totally consumed before gc
  @Override
  protected void finalize() throws Throwable {
    if (this.hasNext()) {
      throw new Exception("a MultiDeviceBatch hasn't been consumed yet");
    }
  }
}
