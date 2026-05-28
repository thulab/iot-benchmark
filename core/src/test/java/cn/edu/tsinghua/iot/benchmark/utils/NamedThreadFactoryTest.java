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

package cn.edu.tsinghua.iot.benchmark.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NamedThreadFactoryTest {

  private static final Runnable NOOP = () -> {};

  @Test
  public void testSingleArgConstructorNamingAndNonDaemon() {
    NamedThreadFactory factory = new NamedThreadFactory("pool");
    Thread t1 = factory.newThread(NOOP);
    Thread t2 = factory.newThread(NOOP);
    Thread t3 = factory.newThread(NOOP);
    assertEquals("pool-thread-1", t1.getName());
    assertEquals("pool-thread-2", t2.getName());
    assertEquals("pool-thread-3", t3.getName());
    assertFalse(t1.isDaemon());
    assertFalse(t2.isDaemon());
    assertFalse(t3.isDaemon());
  }

  @Test
  public void testTwoArgConstructorDaemonTrue() {
    NamedThreadFactory factory = new NamedThreadFactory("daemonPool", true);
    Thread t = factory.newThread(NOOP);
    assertEquals("daemonPool-thread-1", t.getName());
    assertTrue(t.isDaemon());
  }

  @Test
  public void testTwoArgConstructorDaemonFalse() {
    NamedThreadFactory factory = new NamedThreadFactory("normalPool", false);
    Thread t = factory.newThread(NOOP);
    assertEquals("normalPool-thread-1", t.getName());
    assertFalse(t.isDaemon());
  }

  @Test
  public void testIndependentCountersAcrossFactories() {
    NamedThreadFactory factoryA = new NamedThreadFactory("a");
    NamedThreadFactory factoryB = new NamedThreadFactory("b");
    Thread a1 = factoryA.newThread(NOOP);
    Thread a2 = factoryA.newThread(NOOP);
    Thread b1 = factoryB.newThread(NOOP);
    assertEquals("a-thread-1", a1.getName());
    assertEquals("a-thread-2", a2.getName());
    assertEquals("b-thread-1", b1.getName());
  }
}
