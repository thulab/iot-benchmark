# 1. Automation script

## 1.1. One-click script startup
You can use the `cli-benchmark.sh` script to start IoTDB, monitoring IoTDB Benchmark, and testing IoTDB Benchmark in one click, but please note that the script will clean up **all data** in IoTDB when it starts, so please use it with caution.

First, you need to modify the `IOTDB_HOME` parameter in `cli-benchmark.sh` to the folder where your local IoTDB is located.

Then you can use the script to start the test

```sh
> ./cli-benchmark.sh
```

After the test is completed, you can view the test-related logs in the `logs` folder and the monitoring-related logs in the `server-logs` folder.

## 1.2. Automatic execution of multiple tests

Usually, a single test is meaningless unless it is compared with other test results. Therefore, we provide an interface to execute multiple tests with a single startup.

### 1.2.1. Configure routine

Each line of this file should be a parameter that will change during each test (otherwise it becomes a duplicate test). For example, the "routine" file is:

```
LOOP=10 DEVICE_NUMBER=100 TEST
LOOP=20 DEVICE_NUMBER=50 TEST
LOOP=50 DEVICE_NUMBER=20 TEST
```

Then the test process with 3 LOOP parameters of 10, 20, and 50 is executed in sequence.

> Note:
> You can change multiple parameters in each test using the format of "LOOP=20 DEVICE_NUMBER=10 TEST", and unnecessary space is not allowed. The keyword "TEST" means a new test starts. If you change different parameters, the changed parameters will be retained in the next test.

### 1.2.2. Start the test

After configuring the file routine, you can start the multi-test task by launching the script:

```sh
> ./rep-benchmark.sh
```

Then the test information will be displayed in the terminal.

> Note:
> If you close the terminal or lose the connection with the client machine, the test process will terminate. If the output is piped to a terminal, it is the same as in any other case.

Using this interface usually takes a long time and you may want to execute the test process as a daemon. To do so, you can start the test task as a daemon via a startup script:

```sh
> ./rep-benchmark.sh > /dev/null 2>&1 &
```

In this case, if you want to know what is going on, you can view the log information via the following command:

```sh
> cd ./logs
> tail -f log_info.log
```
