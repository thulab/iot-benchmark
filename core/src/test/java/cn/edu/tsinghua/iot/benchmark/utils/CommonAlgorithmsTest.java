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

package cn.edu.tsinghua.iot.benchmark.utils;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CommonAlgorithmsTest {

  private static int sumValues(Map<Integer, Integer> map) {
    int sum = 0;
    for (Integer v : map.values()) {
      sum += v;
    }
    return sum;
  }

  @Test
  public void testDistributeDevicesEvenSplit() {
    Map<Integer, Integer> result = CommonAlgorithms.distributeDevicesToClients(8, 4);
    assertEquals(4, result.size());
    assertEquals(8, sumValues(result));
    for (int i = 0; i < 4; i++) {
      assertEquals(Integer.valueOf(2), result.get(i));
    }
  }

  @Test
  public void testDistributeDevicesUnevenSplit() {
    // From javadoc: 100 devices, 8 clients → {13,13,13,13,12,12,12,12}
    Map<Integer, Integer> result = CommonAlgorithms.distributeDevicesToClients(100, 8);
    assertEquals(8, result.size());
    assertEquals(100, sumValues(result));
    for (int i = 0; i < 4; i++) {
      assertEquals(Integer.valueOf(13), result.get(i));
    }
    for (int i = 4; i < 8; i++) {
      assertEquals(Integer.valueOf(12), result.get(i));
    }
  }

  @Test
  public void testDistributeDevicesFewerThanClients() {
    Map<Integer, Integer> result = CommonAlgorithms.distributeDevicesToClients(3, 8);
    assertEquals(8, result.size());
    assertEquals(3, sumValues(result));
    for (int i = 0; i < 3; i++) {
      assertEquals(Integer.valueOf(1), result.get(i));
    }
    for (int i = 3; i < 8; i++) {
      assertEquals(Integer.valueOf(0), result.get(i));
    }
  }

  @Test
  public void testDistributeDevicesSingleClient() {
    Map<Integer, Integer> result = CommonAlgorithms.distributeDevicesToClients(10, 1);
    assertEquals(1, result.size());
    assertEquals(Integer.valueOf(10), result.get(0));
  }

  @Test
  public void testDistributeDevicesZeroDevices() {
    Map<Integer, Integer> result = CommonAlgorithms.distributeDevicesToClients(0, 4);
    assertEquals(4, result.size());
    assertEquals(0, sumValues(result));
    for (int i = 0; i < 4; i++) {
      assertEquals(Integer.valueOf(0), result.get(i));
    }
  }

  @Test
  public void testDistributeDevicesToTable() {
    // Same strategy as distributeDevicesToClients: 10 devices, 3 tables → {4,3,3}
    Map<Integer, Integer> result = CommonAlgorithms.distributeDevicesToTable(10, 3);
    assertEquals(3, result.size());
    assertEquals(10, sumValues(result));
    assertEquals(Integer.valueOf(4), result.get(0));
    assertEquals(Integer.valueOf(3), result.get(1));
    assertEquals(Integer.valueOf(3), result.get(2));
  }
}
