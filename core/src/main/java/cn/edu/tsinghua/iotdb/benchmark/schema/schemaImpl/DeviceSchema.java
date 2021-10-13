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

package cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl;

import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.utils.ReadWriteIOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeviceSchema implements Cloneable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceSchema.class);
  /** prefix of device name */

  /** Each device belongs to one group, i.e. database */
  private String group;
  /** Name of device, e.g. DEVICE_NAME_PREFIX + deviceId */
  private String device;
  /** Sensors in this device */
  private List<Sensor> sensors;
  /** Only used for synthetic data set */
  private int deviceId;

  public DeviceSchema() {}

  /**
   * @param deviceId e.g. FIRST_DEVICE_INDEX + device
   * @param sensors
   */
  public DeviceSchema(int deviceId, List<Sensor> sensors) {
    this.deviceId = deviceId;
    this.device = MetaUtil.getDeviceName(deviceId);
    this.sensors = sensors;
    try {
      int thisDeviceGroupIndex = MetaUtil.calGroupId(deviceId);
      this.group = MetaUtil.getGroupName(thisDeviceGroupIndex);
    } catch (WorkloadException e) {
      LOGGER.error("Create device schema failed.", e);
    }
  }

  public DeviceSchema(String groupId, String deviceName, List<Sensor> sensors) {
    this.group = MetaUtil.getGroupName(groupId);
    this.device = deviceName;
    this.sensors = sensors;
  }

  public String getDevice() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public int getDeviceId() {
    return deviceId;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public List<Sensor> getSensors() {
    return new ArrayList<>(sensors);
  }

  public void setSensors(List<Sensor> sensors) {
    this.sensors = sensors;
  }

  /**
   * serialize to output stream
   *
   * @param outputStream output stream
   */
  public void serialize(ByteArrayOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(group, outputStream);
    ReadWriteIOUtils.write(device, outputStream);
    ReadWriteIOUtils.write(sensors.size(), outputStream);
    for (Sensor sensor : sensors) {
      sensor.serialize(outputStream);
    }
    ReadWriteIOUtils.write(deviceId, outputStream);
  }

  /**
   * deserialize from input stream
   *
   * @param inputStream input stream
   */
  public static DeviceSchema deserialize(ByteArrayInputStream inputStream) throws IOException {
    DeviceSchema result = new DeviceSchema();
    result.group = ReadWriteIOUtils.readString(inputStream);
    result.device = ReadWriteIOUtils.readString(inputStream);
    result.sensors = new ArrayList<>();
    int number = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < number; i++) {
      result.sensors.add(Sensor.deserialize(inputStream));
    }
    result.deviceId = ReadWriteIOUtils.readInt(inputStream);
    return result;
  }

  @Override
  public String toString() {
    return "DeviceSchema{"
        + "group='"
        + group
        + '\''
        + ", device='"
        + device
        + '\''
        + ", sensors="
        + sensors
        + ", deviceId="
        + deviceId
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof DeviceSchema)) {
      return false;
    }

    DeviceSchema that = (DeviceSchema) o;

    return new EqualsBuilder()
        .append(deviceId, that.deviceId)
        .append(group, that.group)
        .append(device, that.device)
        .append(sensors, that.sensors)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(group)
        .append(device)
        .append(sensors)
        .append(deviceId)
        .toHashCode();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
