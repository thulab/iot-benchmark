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

package cn.edu.tsinghua.iot.benchmark.workload;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Documents the integer-overflow fixes (issue #13) in workload index calculation: why {@code
 * Math.floorMod} replaces {@code Math.abs(...) % n} in {@code GenerateDataWorkLoad.generateOneRow},
 * and why the cast must happen after the modulo in {@code SingletonWorkDataWorkLoad.getOneBatch}.
 * These assertions capture the JDK behaviour that makes the old expressions produce negative
 * (out-of-bounds) indices and the new ones safe.
 */
public class WorkloadIndexTest {

  @Test
  public void testFloorModNeverNegativeOnLongOverflow() {
    int bufferSize = 100;
    // stepOffset * (deviceIndex + 1) can overflow to Long.MIN_VALUE
    long overflowed = Long.MIN_VALUE;
    // old approach was buggy: Math.abs(Long.MIN_VALUE) stays negative -> negative index
    assertTrue("Math.abs(Long.MIN_VALUE) is still negative", Math.abs(overflowed) < 0);
    // fixed approach: floorMod with a positive divisor is always in [0, bufferSize)
    long index = Math.floorMod(overflowed, (long) bufferSize);
    assertTrue("floorMod index out of range", index >= 0 && index < bufferSize);
  }

  @Test
  public void testModuloBeforeCastNeverNegativeDeviceIndex() {
    int deviceNumber = 50;
    long curLoop = (long) Integer.MAX_VALUE + 100; // exceeds int range
    // old approach was buggy: cast happens before modulo -> overflow to negative
    assertTrue("cast-before-mod overflows to negative", ((int) curLoop) % deviceNumber < 0);
    // fixed approach: modulo first keeps the index in [0, deviceNumber)
    int index = (int) (curLoop % deviceNumber);
    assertTrue("mod-before-cast index out of range", index >= 0 && index < deviceNumber);
  }
}
