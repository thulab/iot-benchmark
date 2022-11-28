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

package cn.edu.tsinghua.iotdb.benchmark.tsdb.enums;

public enum DBVersion {
  IOTDB_014("014"),
  IOTDB_013("013"),
  IOTDB_012("012"),
  IOTDB_011("011"),
  IOTDB_010("010"),
  IOTDB_09("09"),
  InfluxDB_2("2.x"),
  TDengine_3("3.x");

  String version;

  DBVersion(String version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return version;
  }
}
