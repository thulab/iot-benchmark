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

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.source.BasicReader;
import cn.edu.tsinghua.iotdb.benchmark.source.file.GenerateCSVReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.interfaces.IRealDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.VerificationQuery;

import java.util.List;

public class RealDataWorkload implements IRealDataWorkload {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private BasicReader basicReader;
  private int batchNumber = 0;

  /** Init reader of real dataset write test */
  public RealDataWorkload(int clientId) {
    List<String> files = MetaUtil.getClientFiles().get(clientId);
    basicReader = new GenerateCSVReader(files);
    batchNumber = files.size() * config.getBIG_BATCH_SIZE();
  }

  /**
   * Return a batch from real data return null if there is no data
   *
   * @return
   * @throws WorkloadException
   */
  @Override
  public Batch getOneBatch() throws WorkloadException {
    if (basicReader.hasNextBatch()) {
      return basicReader.nextBatch();
    } else {
      return null;
    }
  }

  /**
   * Return a verified Query
   *
   * @return
   * @throws WorkloadException
   */
  @Override
  public VerificationQuery getVerifiedQuery() throws WorkloadException {
    Batch batch = getOneBatch();
    if (batch == null) {
      return null;
    }
    return new VerificationQuery(batch);
  }

  @Override
  public int getBatchNumber() {
    return batchNumber;
  }
}
