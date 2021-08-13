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
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** SyntheticWorkload Tester. */
public class SyntheticWorkloadTest {

  private static Config config = ConfigDescriptor.getInstance().getConfig();

  /** Method: getOrderedBatch() */
  @Test
  public void testGetOrderedBatch() throws Exception {
    config.setBATCH_SIZE_PER_WRITE(5);
    config.setPOINT_STEP(5000L);
    config.setIS_REGULAR_FREQUENCY(false);
    SyntheticDataWorkload syntheticWorkload = new SyntheticDataWorkload(1);
    for (int i = 0; i < 3; i++) {
      Batch batch = syntheticWorkload.getOneBatch(new DeviceSchema(1, config.getSENSOR_CODES()), i);
      long old = 0;
      for (Record record : batch.getRecords()) {
        // 检查map里timestamp获取到的是否是按序的
        assertTrue(record.getTimestamp() > old);
        old = record.getTimestamp();
      }
      System.out.println(batch.getRecords().toString());
    }
  }
}
