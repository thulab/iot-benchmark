# 1. 开发者指引
1. iot-benchmark的所有的接口均在core模块中。
2. iot-benchmark的所有的数据库测试的实现均在各个maven子项目中。
3. 如果你想要使用IDEA等编辑器运行Benchmark：
   1. 可以在每一个maven子项目下找到test文件目录下的TestEntrance，运行对应测试。
   2. 以IoTDB 1.0为例，你可以运行`iotdb-1.0/src/main/test/cn/edu/tsinghua/iotdb/benchmark/TestEntrance`
