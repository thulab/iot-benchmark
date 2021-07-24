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

package cn.edu.tsinghua.iotdb.benchmark.iotdb012;

import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;

import java.sql.Connection;
import java.sql.Statement;

public class DoubleIoTDBChecker extends DoubleIOTDB {

  @Override
  public void init() throws TsdbException {
    initConnection();
  }

  @Override
  public void close() throws TsdbException {
    closeConnection();
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    boolean status1 = insertOneConnectionBatch(batch, connection1);
    boolean status2 = insertOneConnectionBatch(batch, connection2);
    if (status1 && status2) {
      return new Status(true);
    } else {
      return new Status(false, 0);
    }
  }

  private boolean insertOneConnectionBatch(Batch batch, Connection connection) {
    try (Statement statement = connection.createStatement()) {
      for (Record record : batch.getRecords()) {
        String sql =
            IoTDB.getInsertOneBatchSql(
                batch.getDeviceSchema(), record.getTimestamp(), record.getRecordDataValue());
        statement.addBatch(sql);
      }
      statement.executeBatch();
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
