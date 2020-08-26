#写入两个IoTDB示例流程

##总体思路
通过benchmark Producer生成数据，写到kafka一个topic中。接下来，启动两次 benchmark consumer，以不同的 consumer group 将数据消费到两个 IoTDB 中。此方案只用来准备数据，并不生成有效报告。


## 安装kafka

https://www.jianshu.com/p/a5b200ce7aae

## 创建topic

bin/kafka-topics.sh --create --zookeeper 127.0.0.1:2181 --topic test-topic --replication-factor 1 --partitions 5

## 一个运行步骤的例子
安装，部署Kafka，启动Kafka和zookeeper，创建topic，partition数要大于写入线程数


### 对Producer
2.1 修改config.propeties文件，配置HOST, PORT, ANOTHER_HOST, ANOTHER_PORT, KAFKA_LOCATION, ZOOKEEPER_LOCATION, TOPIC_NAME,CLIENT_NUMBER 设置 DB_SWITCH=DoubleIoTDB

2.2 运行benchmark.sh
### 对Consumer
3.1 修改pom文件，把IOTDB相关包改为你需要的版本

3.2 修改config.propeties文件, 配置KAFKA_LOCATION, ZOOKEEPER_LOCATION, TOPIC_NAME,CLIENT_NUMBER

3.3 修改bin/startup_consumer.sh, 配置SESSION_HOST, SESSION_PORT, SESSION_USER, SESSION_PASSWORD, KAFKA_GROUP

3.4运行benchmark_consumer.sh启动consumer

3.5 配置、运行第二个IoTDB consumer，注意两者的KAFKA_GROUP必须不同，注意，如果两者共用一个脚本，务必请第一个consumer完全启动后，再修改脚本启动第二个consumer
运行方法和写入完成判断


先运行benchmark.sh启动producer, 再运行benchmark_consumer.sh启动consumer
当producer生产完，client每隔一段时间打印
```
Client session timed out, have not heard from server in 4004ms for sessionid 0x10095f71a220007, closing socket connection and attempting reconnect
```

时，证明消费完了数据，写入完成

## bug
目前测试发现有时候consumer拿不到数据，重启consumer即可，可能是Kafka内部原因？

## 提示
因为写消息队列是异步操作，写数据的性能结果不准确
