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

package cn.edu.tsinghua.iotdb.benchmark.client.operation;

import java.util.ArrayList;
import java.util.List;

public enum Operation {
  INGESTION("INGESTION"),
  PRECISE_QUERY("PRECISE_POINT"),
  RANGE_QUERY("TIME_RANGE"),
  VALUE_RANGE_QUERY("VALUE_RANGE"),
  AGG_RANGE_QUERY("AGG_RANGE"),
  AGG_VALUE_QUERY("AGG_VALUE"),
  AGG_RANGE_VALUE_QUERY("AGG_RANGE_VALUE"),
  GROUP_BY_QUERY("GROUP_BY"),
  LATEST_POINT_QUERY("LATEST_POINT"),
  RANGE_QUERY_ORDER_BY_TIME_DESC("RANGE_QUERY_DESC"),
  VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC("VALUE_RANGE_QUERY_DESC"),
  VERIFICATION_QUERY("VERIFICATION_QUERY"),
  DEVICE_QUERY("DEVICE_QUERY");

  public String getName() {
    return name;
  }

  String name;

  Operation(String name) {
    this.name = name;
  }

  public static List<Operation> getNormalOperation() {
    List<Operation> operations = new ArrayList<>();
    for (Operation operation : Operation.values()) {
      if (operation != Operation.VERIFICATION_QUERY && operation != Operation.DEVICE_QUERY) {
        operations.add(operation);
      }
    }
    return operations;
  }
}
