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

package cn.edu.tsinghua.iotdb.benchmark.entity.enums;

public enum SensorType {
  BOOLEAN("BOOLEAN"),
  INT32("INT32"),
  INT64("INT64"),
  FLOAT("FLOAT"),
  DOUBLE("DOUBLE"),
  TEXT("TEXT");

  public String name;

  SensorType(String name) {
    this.name = name;
  }

  public static SensorType[] getValueTypes() {
    SensorType sensorType[] = new SensorType[4];
    for (int i = 1; i < 5; i++) {
      sensorType[i - 1] = SensorType.values()[i];
    }
    return sensorType;
  }

  public static SensorType getType(int ordinal) {
    for (SensorType sensorType : SensorType.values()) {
      if (sensorType.ordinal() == ordinal) {
        return sensorType;
      }
    }
    // default type
    return SensorType.TEXT;
  }

  @Override
  public String toString() {
    return name;
  }
}
