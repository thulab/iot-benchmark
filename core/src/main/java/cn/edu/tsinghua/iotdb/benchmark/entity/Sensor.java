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

package cn.edu.tsinghua.iotdb.benchmark.entity;

import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.utils.ReadWriteIOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Sensor {
  private String name;
  private SensorType sensorType;

  public Sensor() {}

  public Sensor(String name, SensorType sensorType) {
    this.name = name;
    this.sensorType = sensorType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public SensorType getSensorType() {
    return sensorType;
  }

  public void setSensorType(SensorType sensorType) {
    this.sensorType = sensorType;
  }

  /**
   * serialize to output stream
   *
   * @param outputStream output stream
   */
  public void serialize(ByteArrayOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(sensorType.ordinal(), outputStream);
  }

  /**
   * deserialize from input stream
   *
   * @param inputStream input stream
   */
  public static Sensor deserialize(ByteArrayInputStream inputStream) throws IOException {
    Sensor result = new Sensor();
    result.name = ReadWriteIOUtils.readString(inputStream);
    result.sensorType = SensorType.getType(ReadWriteIOUtils.readInt(inputStream));
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Sensor that = (Sensor) o;

    return new EqualsBuilder()
        .append(name, that.name)
        .append(sensorType, that.sensorType)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(name).append(sensorType).toHashCode();
  }

  @Override
  public String toString() {
    return name;
  }
}
