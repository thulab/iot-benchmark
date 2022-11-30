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

package cn.edu.tsinghua.iot.benchmark.kafka;

import cn.edu.tsinghua.iot.benchmark.entity.Batch;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchConsumeThread implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchConsumeThread.class);
  private final KafkaStream<String, Batch> stream;
  private IDatabase session;

  public BatchConsumeThread(
      KafkaStream<String, Batch> stream, String host, String port, String user, String password)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    this.stream = stream;
    // TODO no hard code
    // Establish session connection of IoTDB
    session =
        (IDatabase) Class.forName("cn.edu.tsinghua.iot.benchmark.iotdb011.IoTDB").newInstance();
  }

  @Override
  public void run() {
    for (MessageAndMetadata<String, Batch> consumerIterator : stream) {
      try {
        session.insertOneBatch(consumerIterator.message());
      } catch (DBConnectException e) {
        LOGGER.error(e.getMessage());
        break;
      }
    }
  }
}
