package cn.edu.tsinghua.iotdb.benchmark.measurement;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.Metric;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.TotalOperationResult;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.TotalResult;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;
import com.clearspring.analytics.stream.quantile.TDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Measurement {

  private static final Logger LOGGER = LoggerFactory.getLogger(Measurement.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static Map<Operation, TDigest> operationLatencyDigest = new EnumMap<>(Operation.class);
  private static Map<Operation, Double> operationLatencySumAllClient = new EnumMap<>(Operation.class);
  private double createSchemaTime;
  private double elapseTime;
  private Map<Operation, Double> operationLatencySumThisClient;
  private Map<Operation, Long> okOperationNumMap;
  private Map<Operation, Long> failOperationNumMap;
  private Map<Operation, Long> okPointNumMap;
  private Map<Operation, Long> failPointNumMap;
  private static final String RESULT_ITEM = "%-20s";
  private static final String LATENCY_ITEM = "%-12s";
  private static final int COMPRESSION = 100;

  static {
    for (Operation operation : Operation.values()) {
      operationLatencyDigest.put(operation, new TDigest(COMPRESSION));
      operationLatencySumAllClient.put(operation, 0D);
    }
  }

  public Measurement() {
    okOperationNumMap = new EnumMap<>(Operation.class);
    failOperationNumMap = new EnumMap<>(Operation.class);
    okPointNumMap = new EnumMap<>(Operation.class);
    failPointNumMap = new EnumMap<>(Operation.class);
    operationLatencySumThisClient = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      okOperationNumMap.put(operation, 0L);
      failOperationNumMap.put(operation, 0L);
      okPointNumMap.put(operation, 0L);
      failPointNumMap.put(operation, 0L);
      operationLatencySumThisClient.put(operation, 0D);
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

  public void addOkPointNum(Operation operation, int pointNum) {
    okPointNumMap.put(operation, okPointNumMap.get(operation) + pointNum);
  }

  public void addFailPointNum(Operation operation, int pointNum) {
    failPointNumMap.put(operation, failPointNumMap.get(operation) + pointNum);
  }

  public void addOkOperationNum(Operation operation) {
    okOperationNumMap.put(operation, okOperationNumMap.get(operation) + 1);
  }

  public void addFailOperationNum(Operation operation) {
    failOperationNumMap.put(operation, failOperationNumMap.get(operation) + 1);
  }

  public double getCreateSchemaTime() {
    return createSchemaTime;
  }

  public void setCreateSchemaTime(double createSchemaTime) {
    this.createSchemaTime = createSchemaTime;
  }

  public double getElapseTime() {
    return elapseTime;
  }

  public void setElapseTime(double elapseTime) {
    this.elapseTime = elapseTime;
  }

  /**
   * users need to call calculateMetrics() after calling mergeMeasurement() to update metrics.
   *
   * @param m measurement to be merged
   */
  public void mergeMeasurement(Measurement m) {
    for (Operation operation : Operation.values()) {
      okOperationNumMap
              .put(operation, okOperationNumMap.get(operation) + m.getOkOperationNum(operation));
      failOperationNumMap
              .put(operation, failOperationNumMap.get(operation) + m.getFailOperationNum(operation));
      okPointNumMap.put(operation, okPointNumMap.get(operation) + m.getOkPointNum(operation));
      failPointNumMap.put(operation, failPointNumMap.get(operation) + m.getFailPointNum(operation));
      // set operationLatencySumThisClient of this measurement the largest latency sum among all threads
      if(operationLatencySumThisClient.get(operation) < m.getOperationLatencySumThisClient().get(operation)) {
        operationLatencySumThisClient.put(operation, m.getOperationLatencySumThisClient().get(operation));
      }
      operationLatencySumAllClient.put(operation,
              operationLatencySumAllClient.get(operation) + m.getOperationLatencySumThisClient().get(operation));
    }

  }

  public void calculateMetrics() {
    for (Operation operation : Operation.values()) {
      double avgLatency = 0;
      if (okOperationNumMap.get(operation) != 0) {
        avgLatency = operationLatencySumAllClient.get(operation) / okOperationNumMap.get(operation);
        Metric.AVG_LATENCY.getTypeValueMap().put(operation, avgLatency);
        Metric.MAX_THREAD_LATENCY_SUM.getTypeValueMap().put(operation, operationLatencySumThisClient.get(operation));
        Metric.MIN_LATENCY.getTypeValueMap().put(operation, operationLatencyDigest.get(operation).quantile(0.0));
        Metric.MAX_LATENCY.getTypeValueMap().put(operation, operationLatencyDigest.get(operation).quantile(1.0));
        Metric.P10_LATENCY.getTypeValueMap().put(operation, operationLatencyDigest.get(operation).quantile(0.1));
        Metric.P25_LATENCY.getTypeValueMap().put(operation, operationLatencyDigest.get(operation).quantile(0.25));
        Metric.MEDIAN_LATENCY.getTypeValueMap().put(operation, operationLatencyDigest.get(operation).quantile(0.5));
        Metric.P75_LATENCY.getTypeValueMap().put(operation, operationLatencyDigest.get(operation).quantile(0.75));
        Metric.P90_LATENCY.getTypeValueMap().put(operation, operationLatencyDigest.get(operation).quantile(0.90));
        Metric.P95_LATENCY.getTypeValueMap().put(operation, operationLatencyDigest.get(operation).quantile(0.95));
        Metric.P99_LATENCY.getTypeValueMap().put(operation, operationLatencyDigest.get(operation).quantile(0.99));
        Metric.P999_LATENCY.getTypeValueMap().put(operation, operationLatencyDigest.get(operation).quantile(0.999));
      }
    }
  }

  public void showMeasurements() {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    ITestDataPersistence recorder = persistenceFactory.getPersistence();
    System.out.println(Thread.currentThread().getName() + " measurements:");
    System.out.println("Create schema cost " + String.format("%.2f", createSchemaTime) + " second");
    System.out.println("Test elapsed time (not include schema creation): " + String.format("%.2f", elapseTime) + " second");
    recorder.saveResult("total", TotalResult.CREATE_SCHEMA_TIME.getName(), "" + createSchemaTime);
    recorder.saveResult("total", TotalResult.ELAPSED_TIME.getName(), "" + elapseTime);

    System.out.println(
            "----------------------------------------------------------Result Matrix----------------------------------------------------------");
    StringBuilder format = new StringBuilder();
    for (int i = 0; i < 6; i++) {
      format.append(RESULT_ITEM);
    }
    format.append("\n");
    System.out.printf(format.toString(), "Operation", "okOperation", "okPoint", "failOperation", "failPoint", "throughput(point/s)");
    for (Operation operation : Operation.values()) {
      String throughput = String.format("%.2f", okPointNumMap.get(operation) / elapseTime);
      System.out.printf(format.toString(), operation.getName(), okOperationNumMap.get(operation), okPointNumMap.get(operation),
              failOperationNumMap.get(operation), failPointNumMap.get(operation), throughput);

      recorder.saveResult(operation.toString(), TotalOperationResult.OK_OPERATION_NUM.getName(), "" + okOperationNumMap.get(operation));
      recorder.saveResult(operation.toString(), TotalOperationResult.OK_POINT_NUM.getName(), "" + okPointNumMap.get(operation));
      recorder.saveResult(operation.toString(), TotalOperationResult.FAIL_OPERATION_NUM.getName(), "" + failOperationNumMap.get(operation));
      recorder.saveResult(operation.toString(), TotalOperationResult.FAIL_POINT_NUM.getName(), "" + failPointNumMap.get(operation));
      recorder.saveResult(operation.toString(), TotalOperationResult.THROUGHPUT.getName(), throughput);
    }
    System.out.println(
            "---------------------------------------------------------------------------------------------------------------------------------");

    recorder.close();
  }

  public void showConfigs() {
    System.out.println("----------------------Main Configurations----------------------");
    System.out.println("DB_SWITCH: " + config.DB_SWITCH);
    System.out.println("OPERATION_PROPORTION: " + config.OPERATION_PROPORTION);
    System.out.println("IS_CLIENT_BIND: " + config.IS_CLIENT_BIND);
    System.out.println("CLIENT_NUMBER: " + config.CLIENT_NUMBER);
    System.out.println("GROUP_NUMBER: " + config.GROUP_NUMBER);
    System.out.println("DEVICE_NUMBER: " + config.DEVICE_NUMBER);
    System.out.println("SENSOR_NUMBER: " + config.SENSOR_NUMBER);
    System.out.println("BATCH_SIZE: " + config.BATCH_SIZE);
    System.out.println("LOOP: " + config.LOOP);
    System.out.println("POINT_STEP: "+ config.POINT_STEP);
    System.out.println("QUERY_INTERVAL: " + config.QUERY_INTERVAL);
    System.out.println("IS_OVERFLOW: " + config.IS_OVERFLOW);
    System.out.println("OVERFLOW_MODE: " + config.OVERFLOW_MODE);
    System.out.println("OVERFLOW_RATIO: " + config.OVERFLOW_RATIO);
    System.out.println("---------------------------------------------------------------");
  }

  public void showMetrics() {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    ITestDataPersistence recorder = persistenceFactory.getPersistence();
    System.out.println(
            "--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------");
    System.out.printf(RESULT_ITEM, "Operation");
    for (Metric metric : Metric.values()) {
      System.out.printf(LATENCY_ITEM, metric.name);
    }
    System.out.println();
    for (Operation operation : Operation.values()) {
      System.out.printf(RESULT_ITEM, operation.getName());
      for (Metric metric : Metric.values()) {
        String metricResult = String.format("%.2f", metric.typeValueMap.get(operation));
        System.out.printf(LATENCY_ITEM, metricResult);
        recorder.saveResult(operation.toString(), metric.name, metricResult);
      }
      System.out.println();
    }
    System.out.println(
            "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    recorder.close();
  }

  public void outputCSV() {

    try {
      String fileName = createFileName();
      File csv = new File(fileName);
      csv.createNewFile();
      outputConfigToCSV(csv);
      outputResultMatrixToCSV(csv);
      outputMetricsToCSV(csv);

    } catch (IOException e) {
      LOGGER.error("Exception occurred during writing csv file because: ", e);
    }
  }

  private String createFileName() {
    // Formatting time
    SimpleDateFormat sdf = new SimpleDateFormat();
    sdf.applyPattern("yyyy-MM-dd-HH-mm-ss");
    Date date = new Date();
    String currentTime = sdf.format(date);

    // Formatting current Operations
    StringBuilder fileNameSB = new StringBuilder();
    String[] operations = config.OPERATION_PROPORTION.split(":");

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

  private void outputConfigToCSV(File csv) {
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
      bw.write("Main Configurations");
      bw.newLine();
      bw.write("DB_SWITCH," + config.DB_SWITCH);
      bw.newLine();
      bw.write("OPERATION_PROPORTION," + config.OPERATION_PROPORTION);
      bw.newLine();
      bw.write("IS_CLIENT_BIND," + config.IS_CLIENT_BIND);
      bw.newLine();
      bw.write("CLIENT_NUMBER," + config.CLIENT_NUMBER);
      bw.newLine();
      bw.write("GROUP_NUMBER," + config.GROUP_NUMBER);
      bw.newLine();
      bw.write("DEVICE_NUMBER," + config.DEVICE_NUMBER);
      bw.newLine();
      bw.write("SENSOR_NUMBER," + config.SENSOR_NUMBER);
      bw.newLine();
      bw.write("BATCH_SIZE," + config.BATCH_SIZE);
      bw.newLine();
      bw.write("LOOP," + config.LOOP);
      bw.newLine();
      bw.write("POINT_STEP,"+ config.POINT_STEP);
      bw.newLine();
      bw.write("QUERY_INTERVAL," + config.QUERY_INTERVAL);
      bw.newLine();
      bw.write("IS_OVERFLOW," + config.IS_OVERFLOW);
      bw.newLine();
      bw.write("OVERFLOW_MODE," + config.OVERFLOW_MODE);
      bw.newLine();
      bw.write("OVERFLOW_RATIO," + config.OVERFLOW_RATIO);
      bw.close();
    } catch (IOException e) {
      LOGGER.error("Exception occurred during operating buffer writer because: ", e);
    }
  }

  private void outputResultMatrixToCSV(File csv) {
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
      bw.newLine();
      bw.write("Result Matrix");
      bw.newLine();
      bw.write("Operation" + "," + "okOperation" + "," + "okPoint" + "," + "failOperation"
              + "," + "failPoint" + "," + "throughput(point/s)");
      for (Operation operation : Operation.values()) {
        String throughput = String.format("%.2f", okPointNumMap.get(operation) / elapseTime);
        bw.newLine();
        bw.write(operation.getName() + "," + okOperationNumMap.get(operation) + "," + okPointNumMap.get(operation)
                + "," + failOperationNumMap.get(operation) + "," + failPointNumMap.get(operation) + "," + throughput);
      }
      bw.close();
    } catch (IOException e) {
      LOGGER.error("Exception occurred during operating buffer writer because: ", e);
    }
  }

  private void outputMetricsToCSV(File csv) {
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
