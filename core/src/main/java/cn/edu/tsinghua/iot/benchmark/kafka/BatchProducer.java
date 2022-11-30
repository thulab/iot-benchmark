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

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class BatchProducer {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  Random random = new Random();

  private Producer<String, Batch> producer;

  public BatchProducer() {

    Properties properties = new Properties();
    properties.put("bootstrap.servers", config.getKAFKA_LOCATION());
    properties.put("zookeeper.connect", config.getZOOKEEPER_LOCATION());
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", BatchSerializer.class.getName());
    properties.put("acks", "-1");
    properties.put("retries", "3");
    properties.put("batch.size", 1024 * 1024);
    properties.put("linger.ms", 10);
    properties.put("buffer.memory", "33554432");
    properties.put("max.block.ms", "3000");

    producer = new KafkaProducer<>(properties);
  }

  /**
   * Send a batch to kafka
   *
   * @param batch batch
   */
  public void send(Batch batch) {
    ProducerRecord<String, Batch> record =
        new ProducerRecord<>(config.getTOPIC_NAME(), String.valueOf(random.nextInt(1000)), batch);
    producer.send(record);
  }

  public void close() {
    producer.close();
  }
}
