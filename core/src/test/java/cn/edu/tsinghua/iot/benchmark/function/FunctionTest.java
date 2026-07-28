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

package cn.edu.tsinghua.iot.benchmark.function;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FunctionTest extends BenchmarkTestBase {

  private static final double DELTA = 1e-9;

  /** SIN/SQUARE/MONO are pure functions of (param, time); RANDOM is excluded (shared Random). */
  private static final String[] DETERMINISTIC = {"DOUBLE_SIN", "DOUBLE_SQUARE", "DOUBLE_MONO"};

  @Test
  public void testDeterministicFunctionsAreReproducible() {
    long time = 12345L;
    for (String type : DETERMINISTIC) {
      FunctionParam param = new FunctionParam(type, 100.0, 0.0, 100L);
      double first = Function.getValueByFunctionIdAndParam(param, time).doubleValue();
      double second = Function.getValueByFunctionIdAndParam(param, time).doubleValue();
      assertEquals(type + " must be reproducible", first, second, DELTA);
    }
  }

  @Test
  public void testBoundedFunctionsStayInRange() {
    double min = 0.0;
    double max = 100.0;
    for (String type : DETERMINISTIC) {
      FunctionParam param = new FunctionParam(type, max, min, 100L);
      for (long time = 0; time < 1000; time += 7) {
        double v = Function.getValueByFunctionIdAndParam(param, time).doubleValue();
        assertTrue(
            type + " value " + v + " out of [" + min + ", " + max + "]",
            v >= min - DELTA && v <= max + DELTA);
      }
    }
  }
}
