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

package cn.edu.tsinghua.iotdb.benchmark.distribution;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/** Only be used under IS_OUT_OF_ORDER=true and OUT_OF_ORDER_MODE=1 */
public class PoissonDistribution {

  private static final Logger LOGGER = LoggerFactory.getLogger(PoissonDistribution.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  /** Lambda in basic model */
  private static final double BASIC_MODEL_LAMBDA = 10;
  /** Boundary of lambda */
  private static final int LAMBDA_BOUNDARY = 500;
  /** MaxK in basic model */
  private static final int BASIC_MODEL_MAX_K = 25;
  /** Get random Double */
  private final Random random;
  /** Lambda in config */
  private double lambda;
  /** MaxK in config */
  private int maxK;

  public PoissonDistribution(Random ran) {
    this.random = ran;
    this.lambda = config.getLAMBDA();
    this.maxK = config.getMAX_K();
  }

  /** Generate next poisson delta */
  public int getNextPoissonDelta() {
    int nextDelta = 0;
    int kInUse = BASIC_MODEL_MAX_K;
    double lambdaInUse = BASIC_MODEL_LAMBDA;
    if (lambda < LAMBDA_BOUNDARY) {
      kInUse = this.maxK;
      lambdaInUse = this.lambda;
    }
    double rand = random.nextDouble();
    // Probability array
    double[] p = new double[kInUse];
    double sum = 0;
    for (int i = 0; i < kInUse - 1; i++) {
      p[i] = getPoissonProbability(i, lambdaInUse);
      sum += p[i];
    }
    p[kInUse - 1] = 1 - sum;
    // Range array
    double[] range = new double[kInUse + 1];
    range[0] = 0;
    for (int i = 0; i < kInUse; i++) {
      range[i + 1] = range[i] + p[i];
    }
    // get nextDelta
    for (int i = 0; i < kInUse; i++) {
      nextDelta++;
      if (isBetween(rand, range[i], range[i + 1])) {
        break;
      }
    }
    if (lambda >= LAMBDA_BOUNDARY) {
      double step;
      if (nextDelta <= BASIC_MODEL_LAMBDA) {
        step = lambda / BASIC_MODEL_LAMBDA;
      } else {
        step = (maxK - lambda) / (BASIC_MODEL_MAX_K - BASIC_MODEL_LAMBDA);
      }
      nextDelta = (int) (lambda + ((nextDelta - BASIC_MODEL_LAMBDA) * step));
    }
    if (nextDelta < 0) {
      LOGGER.warn("Poisson next delta <= 0");
    }
    return nextDelta;
  }

  /** get next poisson probability */
  private double getPoissonProbability(int k, double la) {
    double c = Math.exp(-la);
    double sum = 1;
    for (int i = 1; i <= k; i++) {
      sum *= la / i;
    }
    return sum * c;
  }

  private static boolean isBetween(double a, double b, double c) {
    return a > b && a < c;
  }

  public void setLambda(double lambda) {
    this.lambda = lambda;
  }

  public void setMaxK(int maxK) {
    this.maxK = maxK;
  }
}
