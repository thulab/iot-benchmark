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

import java.util.List;

public interface IBatch {

  /**
   * Use the row protocol which means data are organized in List[timestamp, List[value]]
   *
   * @return the number of cells this batch reserves for one device, i.e. the point count of a dense
   *     write. Under sparse matrix write ({@code NULL_RATIO > 0}) some of those cells are null and
   *     are not written; use {@link #nonNullPointNum()} to count only the written ones.
   */
  long pointNum();

  /**
   * The number of cells this batch actually writes, i.e. {@link #pointNum()} minus the null cells.
   *
   * <p>The measurement layer counts written points with this method, so sparse writes do not
   * inflate the reported throughput. Implementations may return {@link #pointNum()} when no cell of
   * the batch is null.
   *
   * @return the number of non-null cells in this batch
   */
  long nonNullPointNum();

  DeviceSchema getDeviceSchema();

  /**
   * Append deviceSchema and records
   *
   * @param deviceSchema the schema of a device
   * @param records the data which needs to be written
   */
  void addSchemaAndContent(DeviceSchema deviceSchema, List<Record> records);

  void setColIndex(int colIndex);

  int getColIndex();

  List<Record> getRecords();

  /**
   * Check if there are some devices still need to be scanned
   *
   * @return true or false
   */
  boolean hasNext();

  /** Change the index to scan next device */
  void next();

  /**
   * To make sure that MultiDeviceBatch has been totally scanned
   *
   * @throws Exception if scanning hasn't finished
   */
  default void finishCheck() throws Exception {
    if (hasNext()) {
      throw new Exception("batch should have been consumed, but it hasn't. check your code.");
    }
  }

  void reset();
}
