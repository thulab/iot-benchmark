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

package cn.edu.tsinghua.iot.benchmark.distribution;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProbToolTest {

  private static final long SEED = 42L;
  private static final int ITERATIONS = 100;

  @Test
  public void testProbabilityZeroAlwaysFalse() {
    ProbTool tool = new ProbTool();
    Random random = new Random(SEED);
    for (int i = 0; i < ITERATIONS; i++) {
      assertFalse("p=0.0 must never return true", tool.returnTrueByProb(0.0, random));
    }
  }

  @Test
  public void testProbabilityOneAlwaysTrue() {
    ProbTool tool = new ProbTool();
    Random random = new Random(SEED);
    for (int i = 0; i < ITERATIONS; i++) {
      assertTrue("p=1.0 must always return true", tool.returnTrueByProb(1.0, random));
    }
  }

  @Test
  public void testProbabilityHalfMatchesReferenceStream() {
    ProbTool tool = new ProbTool();
    Random toolRandom = new Random(SEED);
    Random referenceRandom = new Random(SEED);
    for (int i = 0; i < ITERATIONS; i++) {
      boolean expected = referenceRandom.nextDouble() < 0.5;
      assertEquals(expected, tool.returnTrueByProb(0.5, toolRandom));
    }
  }
}
