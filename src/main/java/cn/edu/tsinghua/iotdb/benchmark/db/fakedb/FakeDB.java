/**
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

package cn.edu.tsinghua.iotdb.benchmark.db.fakedb;

import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.QueryClientThread;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class FakeDB  implements IDatebase {
  long labID;
  AtomicLong count = new AtomicLong(0);

  public FakeDB() {
    this.labID = 1;
  }
  public FakeDB(long labID) {
    this.labID = labID;
  }

  @Override
  public void init() throws SQLException {
    //do nothing
  }

  @Override
  public void createSchema() throws SQLException {
    //do nothing
  }

  @Override
  public long getLabID() {
    return labID;
  }

  @Override
  public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
    totalTime.set(totalTime.get() + 1000000L);
    latencies.add(1000000L);
  }

  @Override
  public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
    totalTime.set(totalTime.get() + 1000000L);
    latencies.add(1000000L);
  }

  @Override
  public void close() throws SQLException {
    //do nothing
  }

  @Override
  public long getTotalTimeInterval() throws SQLException {
    return 0;
  }

  @Override
  public void executeOneQuery(List<Integer> devices, int index, long startTime,
      QueryClientThread client, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) {
    client.setTotalPoint(client.getTotalPoint() + 1000000L);
    client.setTotalTime(client.getTotalTime() + 1000000L);
    latencies.add(1000000L);
  }

  @Override
  public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex,
      ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies)
      throws SQLException {
    totalTime.set(totalTime.get() + 1000000L);
    latencies.add(1000000L);
  }

  @Override
  public long count(String group, String device, String sensor) {
    return 0;
  }

  @Override
  public void createSchemaOfDataGen() throws SQLException {
    // do nothing
  }

  @Override
  public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
    totalTime.set(totalTime.get() + 1000000L);
    latencies.add(1000000L);
  }

  @Override
  public void exeSQLFromFileByOneBatch() throws SQLException, IOException {
    // do nothing
  }

  @Override
  public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex,
      Random random, ArrayList<Long> latencies) throws SQLException {
    totalTime.set(totalTime.get() + 1000000L);
    latencies.add(1000000L);
    return 1;
  }

  @Override
  public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random,
      ArrayList<Long> latencies) throws SQLException {
    totalTime.set(totalTime.get() + 1000000L);
    latencies.add(1000000L);
    return 1;
  }
}
