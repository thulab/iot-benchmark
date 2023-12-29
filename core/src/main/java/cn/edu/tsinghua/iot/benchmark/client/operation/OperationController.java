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

package cn.edu.tsinghua.iot.benchmark.client.operation;

import cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataMixClient;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OperationController {

  private static final Logger LOGGER = LoggerFactory.getLogger(OperationController.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private List<Double> proportion = new ArrayList<>();
  private List<Operation> operations = Operation.getNormalOperation();
  private boolean isAllWrite = false;
  private Random random;

  public OperationController(int seed) {
    random = new Random(seed);
    String[] split = config.getOPERATION_PROPORTION().split(":");
    if (split.length != Operation.getNormalOperation().size()) {
      LOGGER.error("OPERATION_PROPORTION error, please check this parameter.");
    }
    double[] proportions = new double[Operation.getNormalOperation().size()];
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
    if (Math.abs(proportion.get(0) - 1.0) < 1e-7) {
      isAllWrite = true;
    }
  }

  /**
   * Get next Operation type, using by {@link GenerateDataMixClient}
   *
   * @return Operation the next operation for client to execute
   */
  public Operation getNextOperationType() {
    if (isAllWrite) {
      return Operation.INGESTION;
    }
    // p contains cumulative probability
    double[] p = new double[operations.size() + 1];
    p[0] = 0.0;
    for (int i = 1; i <= operations.size(); i++) {
      p[i] = p[i - 1] + proportion.get(i - 1);
    }
    // use random to getNextOperationType
    double rand = random.nextDouble();
    int i;
    for (i = 1; i <= Operation.getNormalOperation().size(); i++) {
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
      case 12:
        return Operation.GROUP_BY_QUERY_ORDER_BY_TIME_DESC;
      default:
        LOGGER.error("Unsupported operation {}, use default operation: INGESTION.", i);
        return Operation.INGESTION;
    }
  }
}
