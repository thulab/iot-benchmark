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

package cn.edu.tsinghua.iot.benchmark.entity.Batch;

import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.ColumnCategory;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.utils.ReadWriteIOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Batch implements IBatch {
  private DeviceSchema deviceSchema;
  private List<Record> records;
  private int colIndex = -1;

  public Batch() {
    records = new LinkedList<>();
  }

  public Batch(DeviceSchema deviceSchema, List<Record> records) {
    this.deviceSchema = deviceSchema;
    this.records = records;
  }

  @Override
  public long pointNum() {
    long pointNum;
    long measureNum = 0;
    // Count the number of measurements
    List<Sensor> sensors = deviceSchema.getSensors();
    for (Sensor sensor : sensors) {
      if (sensor.getColumnCategory() == ColumnCategory.FIELD) {
        measureNum++;
      }
    }
    pointNum = (measureNum * records.size());
    return pointNum;
  }

  /**
   * The cells this batch actually writes, i.e. {@link #pointNum()} minus the null ones.
   *
   * <p>Under sparse matrix write ({@code NULL_RATIO > 0}) a batch still reserves {@code measureNum
   * * records.size()} cells, but a null cell is omitted from the write request and so must not be
   * reported as a written point. This is what the measurement layer counts, so with {@code
   * NULL_RATIO=0.9} the reported throughput reflects the values that were really written rather
   * than the ten times larger cell capacity.
   *
   * <p>{@link #pointNum()} deliberately keeps returning the reserved cell count: that is the
   * denominator of the sparse ratio, so {@code nonNullPointNum() / pointNum()} still reports the
   * realised density.
   *
   * <p>The count is paired with the schema rather than taken over the raw value list. The IoTDB
   * table model appends its ID columns (device id and tags) to every record inside {@code
   * insertOneBatch}, and the wrapper measures only afterwards; counting raw values would therefore
   * also count those appended identifiers and report more points than {@link #pointNum()} - the
   * very inflation this method exists to prevent. Positions past the schema's sensors are not
   * points, and neither are non-FIELD columns.
   *
   * <p>Nulls are counted from the data, not from {@code NULL_RATIO}: a null can reach a batch
   * without the configuration asking for one, so trusting the config would silently over-report
   * those cells. Walking the values costs far less than the write it is measuring.
   */
  @Override
  public long nonNullPointNum() {
    List<Sensor> sensors = deviceSchema.getSensors();
    long pointNum = 0;
    for (Record record : records) {
      List<Object> values = record.getRecordDataValue();
      int limit = Math.min(values.size(), sensors.size());
      for (int i = 0; i < limit; i++) {
        if (sensors.get(i).getColumnCategory() == ColumnCategory.FIELD && values.get(i) != null) {
          pointNum++;
        }
      }
    }
    return pointNum;
  }

  /**
   * serialize to output stream
   *
   * @param outputStream output stream
   */
  public void serialize(ByteArrayOutputStream outputStream) throws IOException {
    deviceSchema.serialize(outputStream);
    ReadWriteIOUtils.write(records.size(), outputStream);
    for (Record record : records) {
      record.serialize(outputStream);
    }
  }

  /**
   * deserialize from input stream
   *
   * @param inputStream input stream
   */
  public static IBatch deserialize(ByteArrayInputStream inputStream) throws IOException {
    DeviceSchema deviceSchema = DeviceSchema.deserialize(inputStream);
    int size = ReadWriteIOUtils.readInt(inputStream);
    List<Record> records = new LinkedList<>();
    for (int i = 0; i < size; i++) {
      records.add(Record.deserialize(inputStream));
    }

    return new Batch(deviceSchema, records);
  }

  @Override
  public DeviceSchema getDeviceSchema() {
    return deviceSchema;
  }

  @Override
  public void addSchemaAndContent(DeviceSchema deviceSchema, List<Record> records) {
    this.deviceSchema = deviceSchema;
    this.records = records;
  }

  @Override
  public void setColIndex(int colIndex) {
    this.colIndex = colIndex;
  }

  @Override
  public int getColIndex() {
    return colIndex;
  }

  @Override
  public List<Record> getRecords() {
    return records;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public void next() {
    throw new UnsupportedOperationException("SingleDeviceBatch not support next()");
  }

  @Override
  public void reset() {}

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Batch)) {
      return false;
    }

    Batch batch = (Batch) o;

    return new EqualsBuilder()
        .append(deviceSchema, batch.deviceSchema)
        .append(records, batch.records)
        .isEquals();
  }

  @Override
  public String toString() {
    return "Batch{" + "deviceSchema=" + deviceSchema + ", records=" + records + '}';
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(deviceSchema).append(records).toHashCode();
  }
}
