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

package cn.edu.tsinghua.iot.benchmark.schema.schemaImpl;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.utils.ReadWriteIOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeviceSchema implements Cloneable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /** prefix of device name */

  /** Each device belongs to one group, i.e. database */
  private String group;

  /** Each device belongs to one table */
  private String table;

  /** Name of device, e.g. DEVICE_NAME_PREFIX + deviceId */
  private String device;

  /** List of tags */
  private Map<String, String> tags;

  /** Sensors in this device */
  private List<Sensor> sensors;

  /** Only used for synthetic data set */
  private int deviceId;

  public DeviceSchema() {
    tags = new HashMap<>();
  }

  /**
   * @param deviceId e.g. FIRST_DEVICE_INDEX + device
   * @param sensors
   */
  public DeviceSchema(int deviceId, List<Sensor> sensors, Map<String, String> tags) {
    this.deviceId = deviceId;
    this.device = MetaUtil.getDeviceName(deviceId);
    this.sensors = sensors;
    this.tags = tags;
    try {
      int deviceBelongToWhichTable =
          MetaUtil.mappingId(deviceId, config.getDEVICE_NUMBER(), config.getIoTDB_TABLE_NUMBER());
      int tableBelongToWhichGroup =
          MetaUtil.mappingId(
              deviceBelongToWhichTable, config.getIoTDB_TABLE_NUMBER(), config.getGROUP_NUMBER());
      this.table = MetaUtil.getTableName(deviceBelongToWhichTable);
      this.group = MetaUtil.getGroupName(tableBelongToWhichGroup);
    } catch (WorkloadException e) {
      LOGGER.error("Create device schema failed.", e);
    }
  }

  public DeviceSchema(String deviceName, List<Sensor> sensors, Map<String, String> tags) {
    String tableId = MetaUtil.getTableIdFromDeviceName(deviceName);
    this.table = MetaUtil.getTableName(tableId);
    try {
      this.group =
          MetaUtil.getGroupName(
              MetaUtil.mappingId(
                  Integer.parseInt(tableId),
                  config.getIoTDB_TABLE_NUMBER(),
                  config.getGROUP_NUMBER()));
    } catch (WorkloadException e) {
      LOGGER.error("Create device schema failed.", e);
    }
    this.device = deviceName;
    this.sensors = sensors;
    this.tags = tags;
  }

  public DeviceSchema(
      String groupId,
      String tableName,
      String deviceName,
      List<Sensor> sensors,
      Map<String, String> tags) {
    this.group = MetaUtil.getGroupName(groupId);
    this.table = MetaUtil.getTableName(tableName);
    this.device = deviceName;
    this.sensors = sensors;
    this.tags = tags;
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

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
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

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
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
    ReadWriteIOUtils.write(tags.size(), outputStream);
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      ReadWriteIOUtils.write(tag.getKey(), outputStream);
      ReadWriteIOUtils.write(tag.getValue(), outputStream);
    }
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
    int tagNumber = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < tagNumber; i++) {
      String key = ReadWriteIOUtils.readString(inputStream);
      String value = ReadWriteIOUtils.readString(inputStream);
      result.tags.put(key, value);
    }
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
        + ", tags="
        + tags
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
        .append(tags, that.tags)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(group)
        .append(device)
        .append(sensors)
        .append(deviceId)
        .append(tags)
        .toHashCode();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public String getDevicePath() {
    return group + "." + device;
  }
}
