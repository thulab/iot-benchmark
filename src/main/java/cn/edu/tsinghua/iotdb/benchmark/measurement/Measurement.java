package cn.edu.tsinghua.iotdb.benchmark.measurement;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Measurement {

  private static final Logger LOGGER = LoggerFactory.getLogger(Measurement.class);
  private static final double[] MID_AVG_RANGE = {0.1, 0.9};
  private Map<Operation, List<Double>> operationLatencies;
  private Map<Operation, List<Double>> getOperationLatencySumsList;
  private Map<Operation, Double> operationLatencySums;
  private double createSchemaTime;
  private double elapseTime;
  private Map<Operation, Long> okOperationNumMap;
  private Map<Operation, Long> failOperationNumMap;
  private Map<Operation, Long> okPointNumMap;
  private Map<Operation, Long> failPointNumMap;


  public Measurement() {
    operationLatencies = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLatencies.put(operation, new ArrayList<>());
    }
    okOperationNumMap = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      okOperationNumMap.put(operation, 0L);
    }
    failOperationNumMap = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      failOperationNumMap.put(operation, 0L);
    }
    okPointNumMap = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      okPointNumMap.put(operation, 0L);
    }
    failPointNumMap = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      failPointNumMap.put(operation, 0L);
    }
    operationLatencySums = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLatencySums.put(operation, 0D);
    }
    getOperationLatencySumsList = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      getOperationLatencySumsList.put(operation, new ArrayList<>());
    }
  }

  private Map<Operation, Double> getOperationLatencySums() {
    for (Operation operation : Operation.values()) {
      operationLatencySums.put(operation, getDoubleListSum(operationLatencies.get(operation)));
    }
    return operationLatencySums;
  }

  public long getOkOperationNum(Operation operation) {
    return okOperationNumMap.get(operation);
  }

  public long getFailOperationNum(Operation operation) {
    return failOperationNumMap.get(operation);
  }

  public long getOkPointNum(Operation operation) {
    return okPointNumMap.get(operation);
  }

  public long getFailPointNum(Operation operation) {
    return failPointNumMap.get(operation);
  }

  public void addOperationLatency(Operation op, double latency) {
    operationLatencies.get(op).add(latency);
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

  private Map<Operation, List<Double>> getOperationLatencies() {
    return operationLatencies;
  }

  public void setOperationLatencies(
      Map<Operation, List<Double>> operationLatencies) {
    this.operationLatencies = operationLatencies;
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
      operationLatencies.get(operation).addAll(m.getOperationLatencies().get(operation));
      getOperationLatencySumsList.get(operation).add(m.getOperationLatencySums().get(operation));
      okOperationNumMap
          .put(operation, okOperationNumMap.get(operation) + m.getOkOperationNum(operation));
      failOperationNumMap
          .put(operation, failOperationNumMap.get(operation) + m.getFailOperationNum(operation));
      okPointNumMap.put(operation, okPointNumMap.get(operation) + m.getOkPointNum(operation));
      failPointNumMap.put(operation, failPointNumMap.get(operation) + m.getFailPointNum(operation));
    }
  }

  public void calculateMetrics() {
    for (Operation operation : Operation.values()) {
      List<Double> latencyList = operationLatencies.get(operation);
      if (!latencyList.isEmpty()) {
        int totalOps = latencyList.size();
        double sumLatency = 0;
        for (double latency : latencyList) {
          sumLatency += latency;
        }
        double avgLatency = 0;
        avgLatency = sumLatency / totalOps;
        double maxThreadLatencySum = getDoubleListMax(getOperationLatencySumsList.get(operation));
        Metric.MAX_THREAD_LATENCY_SUM.getTypeValueMap().put(operation, maxThreadLatencySum);
        Metric.AVG_LATENCY.getTypeValueMap().put(operation, avgLatency);
        latencyList.sort(new DoubleComparator());
        Metric.MIN_LATENCY.getTypeValueMap().put(operation, latencyList.get(0));
        Metric.MAX_LATENCY.getTypeValueMap().put(operation, latencyList.get(totalOps - 1));
        Metric.P10_LATENCY.getTypeValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.10)));
        Metric.P25_LATENCY.getTypeValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.25)));
        Metric.MEDIAN_LATENCY.getTypeValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.50)));
        Metric.P75_LATENCY.getTypeValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.75)));
        Metric.P90_LATENCY.getTypeValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.90)));
        double midAvgLatency = 0;
        double midSum = 0;
        int midCount = 0;
        for (int i = (int) (totalOps * MID_AVG_RANGE[0]); i < (int) (totalOps * MID_AVG_RANGE[1]);
            i++) {
          midSum += latencyList.get(i);
          midCount++;
        }
        if (midCount != 0) {
          midAvgLatency = midSum / midCount;
        } else {
          LOGGER
              .error("Can not calculate mid-average latency because mid-operation number is zero.");
        }
        Metric.MID_AVG_LATENCY.getTypeValueMap().put(operation, midAvgLatency);
      }
    }
  }

  public void showMeasurements() {
    System.out.println(Thread.currentThread().getName() + " measurements:");

    System.out.println("createSchemaTime in second:");
    System.out.println(String.format("%.2f", createSchemaTime));
    System.out.println("elapseTime in second:");
    System.out.println(String.format("%.2f", elapseTime));

//    for (Operation operation : Operation.values()) {
//      System.out.println("Operation:" + operation);
//      System.out.println("operationLatencies:");
//      System.out.println(
//          operationLatencies.get(operation) + ", size=" + operationLatencies.get(operation).size());
//      System.out.println("operationLatencySums:");
//      System.out.println(operationLatencySums.get(operation));
//      System.out.println("getOperationLatencySumsList:");
//      System.out.println(getOperationLatencySumsList.get(operation));
//    }

    String intervalString = "\t\t";
    System.out.println("Operation\t\tokOperation\tfailOperation\tokPoint\t\tfailPoint");
    for (Operation operation : Operation.values()) {
      System.out.print(operation.getName() + intervalString);
      System.out.print(okOperationNumMap.get(operation) + intervalString);
      System.out.print(failOperationNumMap.get(operation) + intervalString);
      System.out.print(okPointNumMap.get(operation) + intervalString);
      System.out.println(failPointNumMap.get(operation) + intervalString);
    }

  }

  public void showMetrics() {
    String intervalString = "\t\t";
    System.out.print("Operation" + intervalString);
    for (Metric metric : Metric.values()) {
      System.out.print(metric.name + intervalString);
    }
    System.out.println();
    for (Operation operation : Operation.values()) {
      System.out.print(operation.getName() + intervalString);
      for (Metric metric : Metric.values()) {
        System.out
            .print(String.format("%.2f", metric.typeValueMap.get(operation)) + intervalString);
      }
      System.out.println();
    }
  }

  class DoubleComparator implements Comparator<Double> {

    @Override
    public int compare(Double a, Double b) {
      if (a < b) {
        return -1;
      } else if (Objects.equals(a, b)) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  private double getDoubleListSum(List<Double> list) {
    double sum = 0;
    for (double item : list) {
      sum += item;
    }
    return sum;
  }

  private double getDoubleListMax(List<Double> list) {
    double max = 0;
    for (double item : list) {
      max = Math.max(max, item);
    }
    return max;
  }

  public enum Metric {
    MAX_THREAD_LATENCY_SUM("MAX_SUM"),
    AVG_LATENCY("AVG"),
    MID_AVG_LATENCY("MID_AVG"),
    MIN_LATENCY("MIN"),
    P10_LATENCY("P10"),
    P25_LATENCY("P25"),
    P75_LATENCY("P75"),
    MEDIAN_LATENCY("MEDIAN"),
    P90_LATENCY("P90"),
    MAX_LATENCY("MAX");

    public Map<Operation, Double> getTypeValueMap() {
      return typeValueMap;
    }

    Map<Operation, Double> typeValueMap;

    public String getName() {
      return name;
    }

    String name;

    Metric(String name) {
      this.name = name;
      typeValueMap = new EnumMap<>(Operation.class);
      for (Operation operation : Operation.values()) {
        typeValueMap.put(operation, 0D);
      }
    }

  }

}
