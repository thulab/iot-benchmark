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

package cn.edu.tsinghua.iot.benchmark.measurement;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.measurement.enums.Metric;
import cn.edu.tsinghua.iot.benchmark.measurement.enums.TotalOperationResult;
import cn.edu.tsinghua.iot.benchmark.measurement.enums.TotalResult;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.TestDataPersistence;
import com.clearspring.analytics.stream.quantile.TDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

public class Measurement {

  private static final Logger LOGGER = LoggerFactory.getLogger(Measurement.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Map<Operation, TDigest> operationLatencyDigest =
      new EnumMap<>(Operation.class);
  private static final Map<Operation, Double> operationLatencySumAllClient =
      new EnumMap<>(Operation.class);
  private double createSchemaFinishTime = 0;
  private double elapseTime;
  private final Map<Operation, Double> operationLatencySumThisClient;
  private final Map<Operation, Long> okOperationNumMap;
  private final Map<Operation, Long> failOperationNumMap;
  private final Map<Operation, Long> okPointNumMap;
  private final Map<Operation, Long> failPointNumMap;
  private static final String RESULT_ITEM = "%-25s";
  private static final String LATENCY_ITEM = "%-12s";
  /** Precision = 3 / COMPRESSION */
  private static final int COMPRESSION = (int) (300 / config.getRESULT_PRECISION());

  static {
    for (Operation operation : Operation.values()) {
      operationLatencyDigest.put(
          operation, new TDigest(COMPRESSION, new Random(config.getDATA_SEED())));
      operationLatencySumAllClient.put(operation, 0D);
    }
  }

  public Measurement() {
    okOperationNumMap = new EnumMap<>(Operation.class);
    failOperationNumMap = new EnumMap<>(Operation.class);
    okPointNumMap = new EnumMap<>(Operation.class);
    failPointNumMap = new EnumMap<>(Operation.class);
    operationLatencySumThisClient = new EnumMap<>(Operation.class);
    resetMeasurementMaps();
  }

  public void resetMeasurementMaps() {
    for (Operation operation : Operation.values()) {
      okOperationNumMap.put(operation, 0L);
      failOperationNumMap.put(operation, 0L);
      okPointNumMap.put(operation, 0L);
      failPointNumMap.put(operation, 0L);
      operationLatencySumThisClient.put(operation, 0D);
    }
  }

  public void mergeCreateSchemaFinishTime(Measurement m) {
    if (this.createSchemaFinishTime < m.getCreateSchemaFinishTime()) {
      this.createSchemaFinishTime = m.getCreateSchemaFinishTime();
    }
  }

  /**
   * Users need to call calculateMetrics() after calling mergeMeasurement() to update metrics.
   *
   * @param m measurement to be merged
   */
  public void mergeMeasurement(Measurement m) {
    for (Operation operation : Operation.values()) {
      okOperationNumMap.put(
          operation, okOperationNumMap.get(operation) + m.getOkOperationNum(operation));
      failOperationNumMap.put(
          operation, failOperationNumMap.get(operation) + m.getFailOperationNum(operation));
      okPointNumMap.put(operation, okPointNumMap.get(operation) + m.getOkPointNum(operation));
      failPointNumMap.put(operation, failPointNumMap.get(operation) + m.getFailPointNum(operation));

      // set operationLatencySumThisClient of this measurement the largest latency sum among all
      // threads
      if (operationLatencySumThisClient.get(operation)
          < m.getOperationLatencySumThisClient().get(operation)) {
        operationLatencySumThisClient.put(
            operation, m.getOperationLatencySumThisClient().get(operation));
      }
      operationLatencySumAllClient.put(
          operation,
          operationLatencySumAllClient.get(operation)
              + m.getOperationLatencySumThisClient().get(operation));
    }
  }

  /** Calculate metrics of each operation */
  public void calculateMetrics(List<Operation> operations) {
    for (Operation operation : operations) {
      double avgLatency;
      if (okOperationNumMap.get(operation) != 0) {
        avgLatency = operationLatencySumAllClient.get(operation) / okOperationNumMap.get(operation);
        Metric.AVG_LATENCY.getTypeValueMap().put(operation, avgLatency);
        Metric.MAX_THREAD_LATENCY_SUM
            .getTypeValueMap()
            .put(operation, operationLatencySumThisClient.get(operation));
        Function<Double, Double> quantileOrItself =
            (q) -> {
              if (operationLatencyDigest.get(operation).size() > 1) {
                return operationLatencyDigest.get(operation).quantile(q);
              } else {
                // com.clearspring.analytics.stream.quantile.TDigest.quantile needs
                // result size greater than 1 to calculate.
                // If there is only one result, just return this result instead of quantile
                return operationLatencyDigest.get(operation).centroids().iterator().next().mean();
              }
            };
        Metric.MIN_LATENCY.getTypeValueMap().put(operation, quantileOrItself.apply(0.0));
        Metric.MAX_LATENCY.getTypeValueMap().put(operation, quantileOrItself.apply(1.0));
        Metric.P10_LATENCY.getTypeValueMap().put(operation, quantileOrItself.apply(0.1));
        Metric.P25_LATENCY.getTypeValueMap().put(operation, quantileOrItself.apply(0.25));
        Metric.MEDIAN_LATENCY.getTypeValueMap().put(operation, quantileOrItself.apply(0.50));
        Metric.P75_LATENCY.getTypeValueMap().put(operation, quantileOrItself.apply(0.75));
        Metric.P90_LATENCY.getTypeValueMap().put(operation, quantileOrItself.apply(0.90));
        Metric.P95_LATENCY.getTypeValueMap().put(operation, quantileOrItself.apply(0.95));
        Metric.P99_LATENCY.getTypeValueMap().put(operation, quantileOrItself.apply(0.99));
        Metric.P999_LATENCY.getTypeValueMap().put(operation, quantileOrItself.apply(0.999));
      }
    }
  }

  /** Show measurements and record according to TEST_DATA_PERSISTENCE */
  public String getMeasurementsString(List<Operation> operations) {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    TestDataPersistence recorder = persistenceFactory.getPersistence();
    StringBuilder stringBuilder = new StringBuilder("\n");
    stringBuilder
        .append("Create schema cost ")
        .append(String.format("%.2f", createSchemaFinishTime))
        .append(" second")
        .append('\n');
    stringBuilder
        .append("Test elapsed time (not include schema creation): ")
        .append(String.format("%.2f", elapseTime))
        .append(" second")
        .append('\n');
    recorder.saveResultAsync(
        "total", TotalResult.CREATE_SCHEMA_TIME.getName(), "" + createSchemaFinishTime);
    recorder.saveResultAsync("total", TotalResult.ELAPSED_TIME.getName(), "" + elapseTime);

    stringBuilder
        .append(
            "----------------------------------------------------------Result Matrix----------------------------------------------------------")
        .append('\n');
    StringBuffer format = new StringBuffer();
    for (int i = 0; i < 6; i++) {
      format.append(RESULT_ITEM);
    }
    format.append("\n");
    stringBuilder.append(
        String.format(
            format.toString(),
            "Operation",
            "okOperation",
            "okPoint",
            "failOperation",
            "failPoint",
            "throughput(point/s)"));
    for (Operation operation : operations) {
      String throughput = String.format("%.2f", okPointNumMap.get(operation) / elapseTime);
      stringBuilder.append(
          String.format(
              format.toString(),
              operation.getName(),
              okOperationNumMap.get(operation),
              okPointNumMap.get(operation),
              failOperationNumMap.get(operation),
              failPointNumMap.get(operation),
              throughput));

      recorder.saveResultAsync(
          operation.toString(),
          TotalOperationResult.OK_OPERATION_NUM.getName(),
          "" + okOperationNumMap.get(operation));
      recorder.saveResultAsync(
          operation.toString(),
          TotalOperationResult.OK_POINT_NUM.getName(),
          "" + okPointNumMap.get(operation));
      recorder.saveResultAsync(
          operation.toString(),
          TotalOperationResult.FAIL_OPERATION_NUM.getName(),
          "" + failOperationNumMap.get(operation));
      recorder.saveResultAsync(
          operation.toString(),
          TotalOperationResult.FAIL_POINT_NUM.getName(),
          "" + failPointNumMap.get(operation));
      recorder.saveResultAsync(
          operation.toString(), TotalOperationResult.THROUGHPUT.getName(), throughput);
    }
    stringBuilder
        .append(
            "---------------------------------------------------------------------------------------------------------------------------------")
        .append('\n');
    recorder.closeAsync();
    return stringBuilder.toString();
  }

  /** Show Config of test */
  public String getConfigsString() {
    StringBuilder stringBuilder = new StringBuilder("\n");
    stringBuilder
        .append("----------------------Main Configurations----------------------")
        .append('\n');
    stringBuilder.append(config.getShowConfigProperties().toString()).append('\n');
    stringBuilder
        .append("---------------------------------------------------------------")
        .append('\n');
    return stringBuilder.toString();
  }

  /** Show metrics of test */
  public String getMetricsString(List<Operation> operations) {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    TestDataPersistence recorder = persistenceFactory.getPersistence();
    StringBuilder stringBuilder = new StringBuilder("\n");
    stringBuilder
        .append(
            "--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------")
        .append('\n');
    stringBuilder.append(String.format(RESULT_ITEM, "Operation"));
    for (Metric metric : Metric.values()) {
      stringBuilder.append(String.format(LATENCY_ITEM, metric.name));
    }
    stringBuilder.append('\n');
    for (Operation operation : operations) {
      stringBuilder.append(String.format(RESULT_ITEM, operation.getName()));
      for (Metric metric : Metric.values()) {
        String metricResult = String.format("%.2f", metric.typeValueMap.get(operation));
        stringBuilder.append(String.format(LATENCY_ITEM, metricResult));
        recorder.saveResultAsync(operation.toString(), metric.name, metricResult);
      }
      stringBuilder.append('\n');
    }
    stringBuilder
        .append(
            "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------")
        .append('\n');
    recorder.closeAsync();
    return stringBuilder.toString();
  }

  /** output measurement to csv */
  public void outputCSV() {
    MeasurementCsvWriter measurementCsvWriter = new MeasurementCsvWriter();
    measurementCsvWriter.write();
  }

  /** A class which write measurement to csv file */
  private class MeasurementCsvWriter {
    public void write() {
      try {
        String fileName = createFileName();
        File csv = new File(fileName);
        createDirectory();
        csv.createNewFile();
        outputConfigToCSV(csv);
        if (config.isUSE_MEASUREMENT()) {
          outputResultMetricToCSV(csv);
          outputLatencyMetricsToCSV(csv);
          outputSchemaMetricsToCSV(csv);
        }

      } catch (IOException e) {
        LOGGER.error("Exception occurred during writing csv file because: ", e);
      }
    }

    /** Get filename */
    private String createFileName() {
      // Formatting time
      SimpleDateFormat sdf = new SimpleDateFormat();
      sdf.applyPattern("yyyy-MM-dd-HH-mm-ss");
      Date date = new Date();
      String currentTime = sdf.format(date);

      // Formatting current Operations
      StringBuffer fileNameSB = new StringBuffer();
      String[] operations = config.getOPERATION_PROPORTION().split(":");

      for (int i = 0; i < operations.length; i++) {
        // Specify inserting or querying mode
        if (i == 0) {
          fileNameSB.append("I");
        } else if (i == 1) {
          fileNameSB.append("Q");
        }
        // Specify whether a specific operation is processed in this time of test.
        if (!operations[i].equals("0")) {
          if (i == 0) {
            fileNameSB.append("1");
          } else {
            fileNameSB.append(i);
          }
        } else {
          fileNameSB.append("0");
        }
      }
      return "data/csvOutput/" + fileNameSB.toString() + "-" + currentTime + "-test-result.csv";
    }

    /** Create directory if not exists */
    private void createDirectory() {
      File folder = new File("data/csvOutput");
      if (!folder.exists() && !folder.isDirectory()) {
        folder.mkdirs();
      }
    }

    /** Write config to csv */
    private void outputConfigToCSV(File csv) {
      try {
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        bw.write("Main Configurations" + System.lineSeparator());
        bw.write(config.getAllConfigProperties().toString());
        bw.close();
      } catch (IOException e) {
        LOGGER.error("Exception occurred during operating buffer writer because: ", e);
      }
    }

    /** Write result metric to csv */
    private void outputResultMetricToCSV(File csv) {
      try {
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        bw.newLine();
        bw.write("Result Matrix");
        bw.newLine();
        bw.write(
            "Operation"
                + ","
                + "okOperation"
                + ","
                + "okPoint"
                + ","
                + "failOperation"
                + ","
                + "failPoint"
                + ","
                + "throughput(point/s)");
        for (Operation operation : Operation.values()) {
          String throughput = String.format("%.2f", okPointNumMap.get(operation) / elapseTime);
          bw.newLine();
          bw.write(
              operation.getName()
                  + ","
                  + okOperationNumMap.get(operation)
                  + ","
                  + okPointNumMap.get(operation)
                  + ","
                  + failOperationNumMap.get(operation)
                  + ","
                  + failPointNumMap.get(operation)
                  + ","
                  + throughput);
        }
        bw.close();
      } catch (IOException e) {
        LOGGER.error("Exception occurred during operating buffer writer because: ", e);
      }
    }

    /** Write Latency metric to csv */
    private void outputLatencyMetricsToCSV(File csv) {
      try {
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        bw.newLine();
        bw.write("Latency (ms) Matrix");
        bw.newLine();
        bw.write("Operation");
        for (Metric metric : Metric.values()) {
          bw.write("," + metric.name);
        }
        bw.newLine();
        for (Operation operation : Operation.values()) {
          bw.write(operation.getName());
          for (Metric metric : Metric.values()) {
            String metricResult = String.format("%.2f", metric.typeValueMap.get(operation));
            bw.write("," + metricResult);
          }
          bw.newLine();
        }
        bw.close();
      } catch (IOException e) {
        LOGGER.error("Exception occurred during operating buffer writer because: ", e);
      }
    }
  }

  private void outputSchemaMetricsToCSV(File csv) {
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
      bw.newLine();
      bw.write(String.format("Schema cost(s),%.2f", createSchemaFinishTime));
      bw.newLine();
      bw.write(
          String.format("Test elapsed time (not include schema creation)(s),%.2f", elapseTime));
      bw.newLine();
      bw.close();
    } catch (IOException e) {
      LOGGER.error("Exception occurred during operating buffer writer because: ", e);
    }
  }

  private Map<Operation, Double> getOperationLatencySumThisClient() {
    return operationLatencySumThisClient;
  }

  private long getOkOperationNum(Operation operation) {
    return okOperationNumMap.get(operation);
  }

  private long getFailOperationNum(Operation operation) {
    return failOperationNumMap.get(operation);
  }

  private long getOkPointNum(Operation operation) {
    return okPointNumMap.get(operation);
  }

  private long getFailPointNum(Operation operation) {
    return failPointNumMap.get(operation);
  }

  public void addOperationLatency(Operation op, double latency) {
    synchronized (operationLatencyDigest.get(op)) {
      operationLatencyDigest.get(op).add(latency);
    }
    operationLatencySumThisClient.put(op, operationLatencySumThisClient.get(op) + latency);
  }

  public void addOkPointNum(Operation operation, long pointNum) {
    okPointNumMap.put(operation, okPointNumMap.get(operation) + pointNum);
  }

  public void addFailPointNum(Operation operation, long pointNum) {
    failPointNumMap.put(operation, failPointNumMap.get(operation) + pointNum);
  }

  public void addOkOperationNum(Operation operation) {
    okOperationNumMap.put(operation, okOperationNumMap.get(operation) + 1);
  }

  public void addFailOperationNum(Operation operation) {
    failOperationNumMap.put(operation, failOperationNumMap.get(operation) + 1);
  }

  public double getCreateSchemaFinishTime() {
    return createSchemaFinishTime;
  }

  public void setCreateSchemaFinishTime(double createSchemaFinishTime) {
    this.createSchemaFinishTime = createSchemaFinishTime;
  }

  public double getElapseTime() {
    return elapseTime;
  }

  public void setElapseTime(double elapseTime) {
    this.elapseTime = elapseTime;
  }
}
