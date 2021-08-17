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

package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.utils.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class DeviceSchema implements Cloneable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  /** prefix of device name */

  /** Each device belongs to one group, i.e. database */
  private String group;
  /** Name of device, e.g. DEVICE_NAME_PREFIX + deviceId */
  private String device;
  /** Names of sensors from this device, e.g. ["s_0", "s_1", ..., "s_n"] */
  private List<String> sensors;
  /** Only used for synthetic data set */
  private int deviceId;

  public DeviceSchema() {}

  /**
   * @param deviceId e.g. FIRST_DEVICE_INDEX + device
   * @param sensors
   */
  public DeviceSchema(int deviceId, List<String> sensors) {
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

  public DeviceSchema(String group, String device, List<String> sensors) {
    this.group = MetaUtil.getGroupName(group);
    this.device = MetaUtil.getDeviceName(device);
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

  public List<String> getSensors() {
    return sensors;
  }

  public void setSensors(List<String> sensors) {
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
    for (String sensor : sensors) {
      ReadWriteIOUtils.write(sensor, outputStream);
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
    result.sensors = ReadWriteIOUtils.readStringList(inputStream);
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
