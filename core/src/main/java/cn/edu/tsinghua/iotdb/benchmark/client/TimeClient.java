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

package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TimeClient extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeClient.class);
  private Long testMaxTime = ConfigDescriptor.getInstance().getConfig().getTEST_MAX_TIME();

  private List<DataClient> clients;

  public TimeClient(List<DataClient> clients) {
    this.clients = clients;
  }

  @Override
  public void run() {
    if (testMaxTime != 0) {
      super.run();
      boolean finished = false;
      try {
        Thread.sleep(testMaxTime);
      } catch (InterruptedException e) {
        finished = true;
      }
      if (!finished) {
        LOGGER.info("It has been tested for " + testMaxTime + "ms, start to stop all dataClients.");
        for (DataClient client : clients) {
          client.stopClient();
        }
      }
    }
  }
}
