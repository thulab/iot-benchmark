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

package cn.edu.tsinghua.iot.benchmark.utils;

import java.util.HashMap;
import java.util.Map;

public class CommonAlgorithms {
  private CommonAlgorithms() {}

  /**
   * Distribute devices to clients as balance as possible. For example, 100 devices, 8 clients, the
   * result will be {13,13,13,13,12,12,12,12}.
   *
   * @return Map{clientID, how many devices should it take}
   */
  public static Map<Integer, Integer> distributeDevicesToClients(
      final int deviceNumber, final int clientNumber) {
    final int eachClientDeviceNum = deviceNumber / clientNumber;
    final int leftClientDeviceNum = deviceNumber % clientNumber;
    Map<Integer, Integer> result = new HashMap<>();
    for (int clientId = 0; clientId < clientNumber; clientId++) {
      int deviceNum =
          (clientId < leftClientDeviceNum) ? eachClientDeviceNum + 1 : eachClientDeviceNum;
      result.put(clientId, deviceNum);
    }
    return result;
  }

  public static Map<Integer, Integer> distributeDevicesToTablet(
      final int deviceNumber, final int tableNumber) {
    final int eachTableDeviceNum = deviceNumber / tableNumber;
    final int leftTableDeviceNum = deviceNumber % tableNumber;
    Map<Integer, Integer> result = new HashMap<>();
    for (int tableId = 0; tableId < tableNumber; tableId++) {
      int deviceNum = (tableId < leftTableDeviceNum) ? eachTableDeviceNum + 1 : eachTableDeviceNum;
      result.put(tableId, deviceNum);
    }
    return result;
  }
}
