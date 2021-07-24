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

package cn.edu.tsinghua.iotdb.benchmark.measurement.enums;

public enum SystemMetrics {
  /*
   * store the system resource consumption data
   * @param cpu CPU usage
   * @param mem memory usage
   * @param io I/O usage
   * @param networkReceive network receive speed
   * @param networkSend network send speed
   * @param processMemSize memory consumption size of the DB service process
   * @param dataSize data dir size
   * @param systemSize system dir size
   * @param sequenceSize sequence dir size
   * @param overflowSize un-sequence dir size
   * @param walSize WAL dir size
   * @param tps I/O TPS
   * @param ioRead I/O read speed
   * @param ioWrite I/O write speed
   */

  CPU_USAGE,
  MEM_USAGE,
  DISK_IO_USAGE,
  NETWORK_R_RATE,
  NETWORK_S_RATE,
  PROCESS_MEM_SIZE,
  DISK_TPS,
  DISK_READ_SPEED_MB,
  DISK_WRITE_SPEED_MB,

  DATA_FILE_SIZE,
  SYSTEM_FILE_SIZE,
  SEQUENCE_FILE_SIZE,
  UN_SEQUENCE_FILE_SIZE,
  WAL_FILE_SIZE,
}
