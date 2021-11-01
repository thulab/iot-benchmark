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

package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.csv;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.TestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.mode.enums.BenchmarkMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class CSVRecorder extends TestDataPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVRecorder.class);

  /** The count of write retry */
  private static final int WRITE_RETRY_COUNT = 5;
  /** reentrantLock used for writing result into file */
  private static final ReentrantLock reentrantLock = new ReentrantLock(true);

  private static final SimpleDateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private static final SimpleDateFormat projectDateFormat =
      new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");
  private static final long EXP_TIME = System.currentTimeMillis();
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final String projectID =
      String.format(
          "%s_%s_%s_%s",
          config.getBENCHMARK_WORK_MODE(),
          config.getDbConfig().getDB_SWITCH(),
          config.getREMARK(),
          projectDateFormat.format(new java.util.Date(EXP_TIME)));

  /** The name of host */
  private static String localName;
  /** If now line > CSV_MAX_LINE, then the result will write into other files */
  private static final AtomicLong fileNumber = new AtomicLong(1);

  private static AtomicBoolean isRecord = new AtomicBoolean(false);

  static volatile FileWriter projectWriter = null;
  static FileWriter serverInfoWriter = null;
  static FileWriter confWriter = null;
  static FileWriter finalResultWriter = null;
  static String confDir;
  static String dataDir;
  static String csvDir;

  private static final String THREE = ",%s,%s\n";
  private static final String FOUR = ",%s,%s,%s\n";

  public CSVRecorder() {
    try {
      InetAddress localhost = InetAddress.getLocalHost();
      localName = localhost.getHostName();
    } catch (UnknownHostException e) {
      localName = "localName";
      LOGGER.error("Failed to get host name;UnknownHostException：{}", e.getMessage(), e);
    }
    localName = localName.replace("-", "_");
    localName = localName.replace(".", "_");
    Date date = new Date(EXP_TIME);
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
    String day = dateFormat.format(date);
    confDir = System.getProperty(Constants.BENCHMARK_CONF);
    dataDir = confDir.substring(0, confDir.length() - 23) + "/data";
    csvDir = dataDir + "/csv";
    File dataFile = new File(dataDir);
    File csvFile = new File(csvDir);
    if (!dataFile.exists()) {
      if (!dataFile.mkdir()) {
        LOGGER.error("can't create dir");
      }
    }
    if (!csvFile.exists()) {
      if (!csvFile.mkdir()) {
        LOGGER.error("can't create dir");
      }
    }
    try {
      if (config.getBENCHMARK_WORK_MODE() != BenchmarkMode.SERVER) {
        if (!isRecord.get()) {
          confWriter = new FileWriter(csvDir + "/" + projectID + "_CONF.csv", true);
        }
        finalResultWriter = new FileWriter(csvDir + "/" + projectID + "_FINAL_RESULT.csv", true);
        projectWriter = new FileWriter(csvDir + "/" + projectID + "_DETAIL.csv", true);
      } else {
        serverInfoWriter =
            new FileWriter(csvDir + "/SERVER_MODE_" + localName + "_" + day + ".csv", true);
      }
      initCSVFile();
    } catch (IOException e) {
      LOGGER.error("Failed to init csv", e);
      try {
        if (confWriter != null) {
          confWriter.close();
        }
        finalResultWriter.close();
        projectWriter.close();
        if (serverInfoWriter != null) {
          serverInfoWriter.close();
        }
      } catch (IOException ioException) {
        LOGGER.error("", ioException);
      }
    }
  }

  /** write header of csv file */
  public void initCSVFile() throws IOException {
    if (serverInfoWriter != null) {
      String firstLine =
          "id,cpu_usage,mem_usage,diskIo_usage,net_recv_rate,net_send_rate"
              + ",pro_mem_size,dataFileSize,systemFizeSize,sequenceFileSize,unsequenceFileSize"
              + ",walFileSize,tps,MB_read,MB_wrtn\n";
      serverInfoWriter.append(firstLine);
    }
    if (finalResultWriter != null) {
      String firstLine = "id,operation,result_key,result_value\n";
      finalResultWriter.append(firstLine);
    }
    if (config.getBENCHMARK_WORK_MODE() == BenchmarkMode.TEST_WITH_DEFAULT_PATH
        && projectWriter != null) {
      String firstLine =
          "id,recordTime,clientName,operation,okPoint,failPoint,latency,rate,remark\n";
      projectWriter.append(firstLine);
    }
  }

  @Override
  public void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap) {
    String system =
        System.currentTimeMillis()
            + ","
            + systemMetricsMap.get(SystemMetrics.CPU_USAGE)
            + ","
            + systemMetricsMap.get(SystemMetrics.MEM_USAGE)
            + ","
            + systemMetricsMap.get(SystemMetrics.DISK_IO_USAGE)
            + ","
            + systemMetricsMap.get(SystemMetrics.NETWORK_R_RATE)
            + ","
            + systemMetricsMap.get(SystemMetrics.NETWORK_S_RATE)
            + ","
            + systemMetricsMap.get(SystemMetrics.PROCESS_MEM_SIZE)
            + ","
            + systemMetricsMap.get(SystemMetrics.DATA_FILE_SIZE)
            + ","
            + systemMetricsMap.get(SystemMetrics.SYSTEM_FILE_SIZE)
            + ","
            + systemMetricsMap.get(SystemMetrics.SEQUENCE_FILE_SIZE)
            + ","
            + systemMetricsMap.get(SystemMetrics.UN_SEQUENCE_FILE_SIZE)
            + ","
            + systemMetricsMap.get(SystemMetrics.WAL_FILE_SIZE)
            + ","
            + systemMetricsMap.get(SystemMetrics.DISK_TPS)
            + ","
            + systemMetricsMap.get(SystemMetrics.DISK_READ_SPEED_MB)
            + ","
            + systemMetricsMap.get(SystemMetrics.DISK_WRITE_SPEED_MB)
            + "\n";
    try {
      if (serverInfoWriter != null) {
        serverInfoWriter.append(system);
      }
    } catch (IOException e) {
      LOGGER.error("", e);
    }
  }

  @Override
  public void saveTestConfig() {
    StringBuffer str = new StringBuffer("id,configuration_item,configuration_value\n");
    if (config.getBENCHMARK_WORK_MODE() == BenchmarkMode.TEST_WITH_DEFAULT_PATH) {
      str.append(String.format(THREE, "MODE", "DEFAULT_TEST_MODE"));
    }
    for (Map.Entry<String, Object> entry : config.getAllProperties().entrySet()) {
      String value = entry.getValue().toString().replace("\n", "");
      str.append(",").append(entry.getKey()).append(",\"").append(value).append("\"\n");
    }
    try {
      if (!isRecord.get()) {
        confWriter.append(str.toString());
        isRecord.set(true);
      }
    } catch (IOException e) {
      LOGGER.error("", e);
    }
  }

  @Override
  protected void saveOperationResult(
      String operation, int okPoint, int failPoint, double latency, String remark, String device) {
    if (config.isCSV_FILE_SPLIT()) {
      if (config.IncrementAndGetCURRENT_CSV_LINE() >= config.getCSV_MAX_LINE()) {
        reentrantLock.lock();
        try {
          createNewCsvOrInsert(operation, okPoint, failPoint, latency, remark, device);
        } finally {
          reentrantLock.unlock();
        }
      } else {
        insert(operation, okPoint, failPoint, latency, remark, device);
      }
    } else {
      insert(operation, okPoint, failPoint, latency, remark, device);
    }
  }

  private void insert(
      String operation, int okPoint, int failPoint, double latency, String remark, String device) {
    double rate = 0;
    if (latency > 0) {
      // unit: points/second
      rate = okPoint * 1000 / latency;
    }
    String time = dateFormat.format(new java.util.Date(System.currentTimeMillis()));
    String line =
        String.format(
            ",%s,%s,%s,%d,%d,%f,%f,%s\n",
            time, device, operation, okPoint, failPoint, latency, rate, remark);

    // when create a new file writer, old file may be closed.
    int count = 0;
    while (true) {
      try {
        projectWriter.append(line);
        break;
      } catch (IOException e) {
        LOGGER.warn("try to write into old closed file, just try again");
        count++;
        if (count > WRITE_RETRY_COUNT) {
          LOGGER.error("write to file failed", e);
          break;
        }
      }
    }
  }

  private void createNewCsvOrInsert(
      String operation, int okPoint, int failPoint, double latency, String remark, String device) {
    if (config.getCURRENT_CSV_LINE() >= config.getCSV_MAX_LINE()) {
      FileWriter newProjectWriter = null;
      if (config.getBENCHMARK_WORK_MODE() == BenchmarkMode.TEST_WITH_DEFAULT_PATH) {
        String firstLine =
            "id,recordTime,clientName,operation,okPoint,failPoint,latency,rate,remark\n";
        try {
          newProjectWriter =
              new FileWriter(
                  csvDir + "/" + projectID + "_split" + fileNumber.getAndIncrement() + ".csv",
                  true);
          newProjectWriter.append(firstLine);
        } catch (IOException e) {
          LOGGER.error("", e);
        }
      }
      FileWriter oldProjectWriter = projectWriter;
      projectWriter = newProjectWriter;
      try {
        oldProjectWriter.close();
      } catch (IOException e) {
        LOGGER.error("", e);
      }
      config.resetCURRENT_CSV_LINE();
    } else {
      insert(operation, okPoint, failPoint, latency, remark, device);
    }
  }

  @Override
  protected void saveResult(String operation, String key, String value) {
    String line = String.format(FOUR, operation, key, value);
    try {
      finalResultWriter.append(line);
    } catch (IOException e) {
      LOGGER.error("", e);
    }
  }

  /**
   * Use hook to close
   *
   * @see CSVShutdownHook
   */
  @Override
  public void close() {
    // do nothing
  }

  /**
   * close all the writer
   *
   * @see CSVShutdownHook
   */
  public static void readClose() {
    try {
      if (confWriter != null) {
        confWriter.flush();
        confWriter.close();
      }
      if (finalResultWriter != null) {
        finalResultWriter.flush();
        finalResultWriter.close();
      }
      if (projectWriter != null) {
        projectWriter.flush();
        projectWriter.close();
      }
      if (serverInfoWriter != null) {
        serverInfoWriter.flush();
        serverInfoWriter.close();
      }
    } catch (IOException ioException) {
      LOGGER.error("Failed to close writer", ioException);
    }
  }
}
