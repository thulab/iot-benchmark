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

package cn.edu.tsinghua.iot.benchmark.entity;

import cn.edu.tsinghua.iot.benchmark.utils.ReadWriteIOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class Record {

  private long timestamp;
  private List<Object> recordDataValue;

  public Record(long timestamp, List<Object> recordDataValue) {
    this.timestamp = timestamp;
    this.recordDataValue = recordDataValue;
  }

  public int size() {
    return recordDataValue.size();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public List<Object> getRecordDataValue() {
    return recordDataValue;
  }

  /**
   * serialize to output stream
   *
   * @param outputStream output stream
   */
  public void serialize(ByteArrayOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(timestamp, outputStream);
    ReadWriteIOUtils.write(recordDataValue.size(), outputStream);
    for (Object value : recordDataValue) {
      ReadWriteIOUtils.writeObject(value, outputStream);
    }
  }

  /**
   * deserialize from input stream
   *
   * @param inputStream input stream
   */
  public static Record deserialize(ByteArrayInputStream inputStream) throws IOException {
    long timestamp = ReadWriteIOUtils.readLong(inputStream);
    return new Record(timestamp, ReadWriteIOUtils.readObjectList(inputStream));
  }

  @Override
  public String toString() {
    return "Record{" + "timestamp=" + timestamp + ", recordDataValue=" + recordDataValue + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Record)) {
      return false;
    }

    Record record = (Record) o;

    return new EqualsBuilder()
        .append(timestamp, record.timestamp)
        .append(recordDataValue, record.recordDataValue)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(timestamp).append(recordDataValue).toHashCode();
  }
}
