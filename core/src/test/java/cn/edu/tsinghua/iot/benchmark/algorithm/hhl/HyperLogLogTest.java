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

package cn.edu.tsinghua.iot.benchmark.algorithm.hhl;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import net.agkn.hll.HLL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.weakref.jmx.internal.guava.hash.HashFunction;
import org.weakref.jmx.internal.guava.hash.Hashing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class HyperLogLogTest {

  @Before
  public void before() {}

  @After
  public void after() {}

  @Test
  public void testAgknHll() {
    final int seed = 12345;
    HashFunction hash = Hashing.murmur3_128(seed);
    // data on which to calculate distinct count
    Random random = new Random(seed);
    int sampleSize = 10000;
    double[] floor = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0};
    String[] avgErrors = new String[floor.length];
    int testNum = 1000;

    for (int floorIndex = 0; floorIndex < floor.length; floorIndex++) {
      double errorSum = 0;
      for (int testIndex = 0; testIndex < testNum; testIndex++) {
        final ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < sampleSize; i++) {
          list.add(random.nextInt((int) (sampleSize * floor[floorIndex])));
        }
        Set<Integer> set = new HashSet<>();
        final HLL hll = new HLL(13, 5); // number of bucket and bits per bucket
        for (int a : list) {

          set.add(a);

          final long value = hash.newHasher().putInt(a).hash().asLong();
          hll.addRaw(value);
        }

        double p = (hll.cardinality() - set.size()) / (double) set.size();
        errorSum += Math.abs(p);
      }
      avgErrors[floorIndex] = String.format("%.5f", errorSum / testNum);
    }
    System.out.println(Arrays.toString(avgErrors));
    // output: [0.00600, 0.00244, 0.00230, 0.01021, 0.00874, 0.00809, 0.00657, 0.00546, 0.00615,
    // 0.00642, 0.00616, 0.00690, 0.00714, 0.00771, 0.00707]
    // time cost: 26s
  }

  @Test
  public void testStreamlibHll() {
    final int seed = 12345;
    // data on which to calculate distinct count
    Random random = new Random(seed);
    int sampleSize = 10000;
    double[] floor = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0};
    String[] avgErrors = new String[floor.length];
    int testNum = 1000;

    for (int floorIndex = 0; floorIndex < floor.length; floorIndex++) {
      double errorSum = 0;
      for (int testIndex = 0; testIndex < testNum; testIndex++) {
        final ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < sampleSize; i++) {
          list.add(random.nextInt((int) (sampleSize * floor[floorIndex])));
        }
        Set<Integer> set = new HashSet<>();
        ICardinality card = new HyperLogLog(13);
        for (int a : list) {
          set.add(a);
          card.offer(a);
        }

        double p = (card.cardinality() - set.size()) / (double) set.size();
        errorSum += Math.abs(p);
      }
      avgErrors[floorIndex] = String.format("%.5f", errorSum / testNum);
    }
    System.out.println(Arrays.toString(avgErrors));
    // output: [0.00299, 0.00914, 0.00331, 0.00294, 0.00348, 0.00450, 0.00605, 0.00581, 0.00580,
    // 0.00585, 0.00668, 0.00672, 0.00748, 0.00753, 0.00742]
    // time cost: 11s
  }

  @Test
  public void testHllBench() {
    final int seed = 12345;
    HashFunction hash = Hashing.murmur3_128(seed);
    // data on which to calculate distinct count
    Random random = new Random(seed);
    int sampleSize = 100000000;
    double floor = 0.1;
    System.out.println("sample size: " + sampleSize);
    int a = (int) (sampleSize * floor);

    long start = System.currentTimeMillis();
    ICardinality card = new HyperLogLog(13);
    for (int i = 0; i < sampleSize; i++) {
      card.offer(random.nextInt(a));
    }
    long p = card.cardinality();
    long elapsed = System.currentTimeMillis() - start;
    System.out.println("streamlib p = " + p);
    System.out.println("streamlib cost " + elapsed + " ms");

    random = new Random(seed);
    start = System.currentTimeMillis();
    final HLL hll = new HLL(13, 5); // number of bucket and bits per bucket
    for (int i = 0; i < sampleSize; i++) {
      final long value = hash.newHasher().putInt(random.nextInt(a)).hash().asLong();
      hll.addRaw(value);
    }
    p = hll.cardinality();
    long elapsed2 = System.currentTimeMillis() - start;
    System.out.println("agkn p = " + p);
    System.out.println("agkn cost " + elapsed2 + " ms");

    System.out.println("agkn/streamlib " + ((double) elapsed2 / elapsed));
  }
}
