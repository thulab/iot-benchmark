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

  private double getDoubleListSum(List<Double> list) {
    double sum = 0;
    for(double item: list){
      sum += item;
    }
    return sum;
  }

  private double getDoubleListMax(List<Double> list) {
    double max = 0;
    for(double item: list){
      max = Math.max(max, item);
    }
    return max;
  }

  private Map<Operation, Double> getOperationLatencySums() {
    for(Operation operation: Operation.values()){
      operationLatencySums.put(operation, getDoubleListSum(operationLatencies.get(operation)));
    }
    return operationLatencySums;
  }

  private Map<Operation, Double> operationLatencySums;
  private double createSchemaTime;
  private double elapseTime;
  private Map<Operation, Long> okOperationNumMap;
  private Map<Operation, Long> failOperationNumMap;
  private Map<Operation, Long> okPointNumMap;
  private Map<Operation, Long> failPointNumMap;


  public long getOkOperationNum(Operation operation){
    return okOperationNumMap.get(operation);
  }

  public long getFailOperationNum(Operation operation){
    return failOperationNumMap.get(operation);
  }

  public long getOkPointNum(Operation operation){
    return okPointNumMap.get(operation);
  }

  public long getFailPointNum(Operation operation){
    return failPointNumMap.get(operation);
  }

  public void addOkPointNum(Operation operation, int pointNum){
    okPointNumMap.put(operation, okPointNumMap.get(operation) + pointNum);
  }

  public void addFailPointNum(Operation operation, int pointNum){
    failPointNumMap.put(operation, failPointNumMap.get(operation) + pointNum);
  }

  public void addOkOperationNum(Operation operation) {
    okOperationNumMap.put(operation, okOperationNumMap.get(operation) + 1);
  }

  public void addFailOperationNum(Operation operation) {
    failOperationNumMap.put(operation, failOperationNumMap.get(operation) + 1);
  }

  public Measurement() {
    operationLatencies = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLatencies.put(operation, new ArrayList<>());
    }
    okOperationNumMap = new EnumMap<>(Operation.class);
    failOperationNumMap = new EnumMap<>(Operation.class);
    okPointNumMap = new EnumMap<>(Operation.class);
    failPointNumMap = new EnumMap<>(Operation.class);
    operationLatencySums = new EnumMap<>(Operation.class);
    getOperationLatencySumsList = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      getOperationLatencySumsList.put(operation, new ArrayList<>());
    }
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
    }
  }

  public void addOperationLatency(Operation op, double latency) {
    operationLatencies.get(op).add(latency);
  }

  public void calculateMetrics() {
    for (Operation operation : Operation.values()) {
      List<Double> latencyList = operationLatencies.get(operation);
      int totalOps = latencyList.size();
      double sumLatency = 0;
      for (double latency : latencyList) {
        sumLatency += latency;
      }
      double avgLatency = 0;
      if (totalOps != 0) {
        avgLatency = sumLatency / totalOps;
      } else {
        LOGGER.error("Can not calculate average latency because total operation number is zero.");
      }
      double maxThreadLatencySum = getDoubleListMax(getOperationLatencySumsList.get(operation));
      Metric.MAX_THREAD_LATENCY_SUM.getTypeValueMap().put(operation, maxThreadLatencySum);
      Metric.AVG_LATENCY.getTypeValueMap().put(operation, avgLatency);
      latencyList.sort(new DoubleComparator());
      Metric.MIN_LATENCY.getTypeValueMap().put(operation, latencyList.get(0));
      Metric.MAX_LATENCY.getTypeValueMap().put(operation, latencyList.get(totalOps - 1));
      Metric.P10_LATENCY.getTypeValueMap().put(operation, latencyList.get((int) (totalOps * 0.10)));
      Metric.P25_LATENCY.getTypeValueMap().put(operation, latencyList.get((int) (totalOps * 0.25)));
      Metric.MEDIAN_LATENCY.getTypeValueMap()
          .put(operation, latencyList.get((int) (totalOps * 0.50)));
      Metric.P75_LATENCY.getTypeValueMap().put(operation, latencyList.get((int) (totalOps * 0.75)));
      Metric.P90_LATENCY.getTypeValueMap().put(operation, latencyList.get((int) (totalOps * 0.90)));
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
        LOGGER.error("Can not calculate mid-average latency because mid-operation number is zero.");
      }
      Metric.MID_AVG_LATENCY.getTypeValueMap().put(operation, midAvgLatency);
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

  public enum Metric {
    MAX_THREAD_LATENCY_SUM,
    AVG_LATENCY,
    MEDIAN_LATENCY,
    MID_AVG_LATENCY,
    MIN_LATENCY,
    P10_LATENCY,
    P25_LATENCY,
    P75_LATENCY,
    P90_LATENCY,
    MAX_LATENCY;

    public Map<Operation, Double> getTypeValueMap() {
      return typeValueMap;
    }

    Map<Operation, Double> typeValueMap;

    Metric() {
      typeValueMap = new EnumMap<>(Operation.class);
    }

  }

}
