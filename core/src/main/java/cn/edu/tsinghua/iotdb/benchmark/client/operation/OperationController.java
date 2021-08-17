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

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.mode.enums.BenchmarkMode;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OperationController {

  private static final Logger LOGGER = null;
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private Random random;

  public OperationController(int seed) {
    random = new Random(seed);
  }

  /**
   * Get next Operation type, using by {@link
   * cn.edu.tsinghua.iotdb.benchmark.client.generate.SyntheticClient}
   *
   * @return Operation the next operation for client to execute
   */
  public Operation getNextOperationType() {
    List<Double> proportion = resolveOperationProportion();
    // p contains cumulative probability
    double[] p = new double[Operation.values().length + 1];
    p[0] = 0.0;
    for (int i = 1; i <= Operation.values().length; i++) {
      p[i] = p[i - 1] + proportion.get(i - 1);
    }
    // use random to getNextOperationType
    double rand = random.nextDouble();
    int i;
    for (i = 1; i <= Operation.values().length; i++) {
      if (rand >= p[i - 1] && rand <= p[i]) {
        break;
      }
    }
    switch (i) {
      case 1:
        return Operation.INGESTION;
      case 2:
        return Operation.PRECISE_QUERY;
      case 3:
        return Operation.RANGE_QUERY;
      case 4:
        return Operation.VALUE_RANGE_QUERY;
      case 5:
        return Operation.AGG_RANGE_QUERY;
      case 6:
        return Operation.AGG_VALUE_QUERY;
      case 7:
        return Operation.AGG_RANGE_VALUE_QUERY;
      case 8:
        return Operation.GROUP_BY_QUERY;
      case 9:
        return Operation.LATEST_POINT_QUERY;
      case 10:
        return Operation.RANGE_QUERY_ORDER_BY_TIME_DESC;
      case 11:
        return Operation.VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC;
      default:
        LOGGER.error("Unsupported operation {}, use default operation: INGESTION.", i);
        return Operation.INGESTION;
    }
  }

  /**
   * calculate proportion according to OPERATION_PROPORTION
   *
   * @return
   */
  private List<Double> resolveOperationProportion() {
    List<Double> proportion = new ArrayList<>();
    String[] split = config.getOPERATION_PROPORTION().split(":");
    if (split.length != Operation.values().length) {
      LOGGER.error("OPERATION_PROPORTION error, please check this parameter.");
    }
    if (config.getBENCHMARK_WORK_MODE() != BenchmarkMode.VERIFICATION_QUERY
        && config.getBENCHMARK_WORK_MODE() != BenchmarkMode.VERIFICATION_WRITE) {
      split[11] = "0";
    } else {
      split[11] = "1";
    }
    double[] proportions = new double[Operation.values().length];
    double sum = 0;
    for (int i = 0; i < split.length; i++) {
      proportions[i] = Double.parseDouble(split[i]);
      sum += proportions[i];
    }
    for (int i = 0; i < split.length; i++) {
      if (sum != 0) {
        proportion.add(proportions[i] / sum);
      } else {
        proportion.add(0.0);
        LOGGER.error("The sum of operation proportions is zero!");
      }
    }
    return proportion;
  }
}
