# 1. Developer Guide
1. All interfaces of iot-benchmark are in the core module.
2. All database tests of iot-benchmark are implemented in various Maven subprojects.
3. If you want to run Benchmark using editors such as IDEA:
    -- You can find TestEntrance in the test file directory under each Maven subproject and run the corresponding test.
    -- Taking IoTDB 1.0 as an example, you can run `iotdb-1.0/src/main/test/cn/edu/tsinghua/iotdb/benchmark/TestEntrance`
