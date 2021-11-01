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

package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.source.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RealDataWorkLoad extends DataWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealDataWorkLoad.class);
  private DataReader dataReader;
  private final long batchNumber;

  public RealDataWorkLoad(List<String> files) {
    dataReader = DataReader.getInstance(files);
    batchNumber = files.size() * config.getBIG_BATCH_SIZE();
  }

  @Override
  public Batch getOneBatch() throws WorkloadException {
    if (dataReader.hasNextBatch()) {
      Batch batch = dataReader.nextBatch();
      if (config.isIS_RECENT_QUERY()) {
        for (Record record : batch.getRecords()) {
          currentTimestamp = Math.max(currentTimestamp, record.getTimestamp());
        }
      }
      return batch;
    } else {
      return null;
    }
  }

  @Override
  public long getBatchNumber() {
    return batchNumber;
  }
}
